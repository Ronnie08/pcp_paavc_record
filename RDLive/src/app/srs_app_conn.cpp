/*
The MIT License (MIT)

Copyright (c) 2013-2015 SRS(ossrs)

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

#include <srs_app_conn.hpp>

using namespace std;

#include <srs_kernel_log.hpp>
#include <srs_kernel_error.hpp>
#include <srs_app_utility.hpp>
#include <srs_kernel_utility.hpp>

IConnectionManager::IConnectionManager()
{
}

IConnectionManager::~IConnectionManager()
{
}

SrsConnection::SrsConnection(IConnectionManager* cm, st_netfd_t c)
{
    id = 0;
    manager = cm;
    stfd = c;
    disposed = false;
    expired = false;
    
    // the client thread should reap itself, 
    // so we never use joinable.
    // TODO: FIXME: maybe other thread need to stop it.
    // @see: https://github.com/ossrs/srs/issues/78
    pthread = new SrsOneCycleThread("conn", this);
}

SrsConnection::~SrsConnection()
{
    dispose();
    
    srs_freep(pthread);
}

void SrsConnection::dispose()
{
    if (disposed) {
        return;
    }
    
    disposed = true;
    
    /**
     * when delete the connection, stop the connection,
     * close the underlayer socket, delete the thread.
     */
    srs_close_stfd(stfd);
}

int SrsConnection::start()
{
    return pthread->start();
}

int SrsConnection::cycle()
{
    int ret = ERROR_SUCCESS;
    
    _srs_context->generate_id();
    id = _srs_context->get_id();
    
    ip = srs_get_peer_ip(st_netfd_fileno(stfd));
    
    ret = do_cycle();
    
	// if socket elb heartbeat, set to closed
	if (srs_is_client_elb_heartbeat_close(ret)) {
		srs_trace("elb heartbeat disconnect peer. ret=%d", ret);
	}

    // if socket io error, set to closed.
    if (srs_is_client_gracefully_close(ret)) {
        srs_warn("reset client peer. ret=%d to closed", ret);
        ret = ERROR_SOCKET_CLOSED;
    }
    
    // success.
    if (ret == ERROR_SUCCESS) {
        srs_trace("client finished.");
    }
    
    // client close peer.
    if (ret == ERROR_SOCKET_CLOSED) {
        srs_warn("client disconnect peer. ret=%d", ret);
    }

    return ERROR_SUCCESS;
}

void SrsConnection::on_thread_stop()
{
    // TODO: FIXME: never remove itself, use isolate thread to do cleanup.
    manager->remove(this);
}

int SrsConnection::srs_id()
{
    return id;
}

string SrsConnection::remote_ip() {
    return ip;
}

void SrsConnection::expire()
{
    expired = true;
	wakeup();
}

void SrsConnection::wakeup()
{
    
}

SrsScanM3u8Thread::SrsScanM3u8Thread(ISrsHttpResponseWriter* w,ISrsHttpMessage* r,const std::string pattern,const std::string dir,const std::string path)
{
    writer  = w;
    req = r;
    pattern_ = pattern;
    dir_ = dir;
    path_ = path;
    running_ = true;
    exit_ = false;
    error = ERROR_SUCCESS;
    trd = new SrsOneCycleThread("scan-m3u8", this);
}

SrsScanM3u8Thread::~SrsScanM3u8Thread()
{
    srs_freep(trd);
}

int SrsScanM3u8Thread::start()
{
    return trd->start();
}

int SrsScanM3u8Thread::error_code()
{
    return error;
}

void SrsScanM3u8Thread::stop() {
    running_ = false;
    while(!exit_) {
        st_usleep(100*1000);
    }
}
int SrsScanM3u8Thread::cycle()
{
    int ret = ERROR_SUCCESS;
    
    std::string upath = path_;
    
    // add default pages.
    if (srs_string_ends_with(upath, "/")) {
        upath += "index.html";
    }
    
    std::string fullpath = dir_ + "/";
    
    // remove the virtual directory.
    size_t pos = pattern_.find("/");
    if (upath.length() > pattern_.length() && pos != std::string::npos) {
        fullpath += upath.substr(pattern_.length() - pos);
    } else {
        fullpath += upath;
    }
    
    int times = 0;
    while (running_) {
        // stat current dir, if exists, return error.
        if (!srs_path_exists(fullpath)) {
            srs_warn("http miss file=%s, pattern=%s, upath=%s",
                 fullpath.c_str(), pattern_.c_str(), upath.c_str());
        } else {
            ret = SrsHttpFoundHandler().serve_http(writer, req,fullpath);
            break;
        }
        times++;
        if (times> 10)
            break;
        st_usleep(1000*1000);
    }
    if (times>10) {
      ret =   SrsHttpNotFoundHandler().serve_http(writer, req);
    }
    exit_ = true;
    return ret;
}
