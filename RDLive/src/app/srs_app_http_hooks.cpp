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

#include <srs_app_http_hooks.hpp>

#ifdef SRS_AUTO_HTTP_CALLBACK

#include <sstream>
using namespace std;

#include <srs_kernel_error.hpp>
#include <srs_rtmp_stack.hpp>
#include <srs_app_st.hpp>
#include <srs_protocol_json.hpp>
#include <srs_app_dvr.hpp>
#include <srs_app_http_client.hpp>
#include <srs_core_autofree.hpp>
#include <srs_app_config.hpp>
#include <srs_kernel_utility.hpp>
#include <srs_app_http_conn.hpp>
#include <srs_app_utility.hpp>

#define SRS_HTTP_RESPONSE_OK    SRS_XSTR(ERROR_SUCCESS)

#define SRS_HTTP_HEADER_BUFFER        1024
#define SRS_HTTP_READ_BUFFER    4096
#define SRS_HTTP_BODY_BUFFER        32 * 1024

// the timeout for hls notify, in us.
#define SRS_HLS_NOTIFY_TIMEOUT_US (int64_t)(10*1000*1000LL)

SrsHttpHooks::SrsHttpHooks()
{
}

SrsHttpHooks::~SrsHttpHooks()
{
}

int SrsHttpHooks::on_connect(string url, SrsRequest* req)
{
    int ret = ERROR_SUCCESS;
    
    int client_id = _srs_context->get_id();
    
    std::stringstream ss;
    ss << SRS_JOBJECT_START
        << SRS_JFIELD_STR("action", "on_connect") << SRS_JFIELD_CONT
        << SRS_JFIELD_ORG("client_id", client_id) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("ip", req->ip) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("app", req->app) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("tcUrl", req->tcUrl) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("pageUrl", req->pageUrl)
        << SRS_JOBJECT_END;
        
    std::string data = ss.str();
    std::string res;
    int status_code;
    if ((ret = do_post(url, data, status_code, res)) != ERROR_SUCCESS) {
        srs_error("http post on_connect uri failed. "
            "client_id=%d, url=%s, request=%s, response=%s, code=%d, ret=%d",
            client_id, url.c_str(), data.c_str(), res.c_str(), status_code, ret);
        return ret;
    }
    
    srs_trace("http hook on_connect success. "
        "client_id=%d, url=%s, request=%s, response=%s, ret=%d",
        client_id, url.c_str(), data.c_str(), res.c_str(), ret);
    
    return ret;
}

void SrsHttpHooks::on_close(string url, SrsRequest* req, int64_t send_bytes, int64_t recv_bytes)
{
    int ret = ERROR_SUCCESS;
    
    int client_id = _srs_context->get_id();
    
    std::stringstream ss;
    ss << SRS_JOBJECT_START
        << SRS_JFIELD_STR("action", "on_close") << SRS_JFIELD_CONT
        << SRS_JFIELD_ORG("client_id", client_id) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("ip", req->ip) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
        << SRS_JFIELD_ORG("send_bytes", send_bytes) << SRS_JFIELD_CONT
        << SRS_JFIELD_ORG("recv_bytes", recv_bytes) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("app", req->app)
        << SRS_JOBJECT_END;
        
    std::string data = ss.str();
    std::string res;
    int status_code;
    if ((ret = do_post(url, data, status_code, res)) != ERROR_SUCCESS) {
        srs_warn("http post on_close uri failed, ignored. "
            "client_id=%d, url=%s, request=%s, response=%s, code=%d, ret=%d",
            client_id, url.c_str(), data.c_str(), res.c_str(), status_code, ret);
        return;
    }
    
    srs_trace("http hook on_close success. "
        "client_id=%d, url=%s, request=%s, response=%s, ret=%d",
        client_id, url.c_str(), data.c_str(), res.c_str(), ret);
    
    return;
}

int SrsHttpHooks::on_publish(string url, SrsRequest* req)
{
    int ret = ERROR_SUCCESS;
    
    int client_id = _srs_context->get_id();
   
    int64_t current_time = srs_update_system_time_ms();
    std::stringstream time_ss;
    time_ss << current_time;
 
    std::string domain = srs_get_param_key(req->param,"domain");    
    std::stringstream ss;
   
    std::string http_header="";
    
    SrsConfDirective* conf_key = _srs_config->get_vhost_http_header_key(req->vhost);
    SrsConfDirective* conf_value = _srs_config->get_vhost_http_header_value(req->vhost);
    if(conf_key && conf_value) {
        std::stringstream str_header;
        str_header << conf_key->args[0]
                   << ": "
                   << conf_value->args[0];
        http_header = str_header.str();
    }
    


    /*ss << SRS_JOBJECT_START
        << SRS_JFIELD_STR("action", "on_publish") << SRS_JFIELD_CONT
        << SRS_JFIELD_ORG("client_id", client_id) << SRS_JFIELD_CONT
        << SRS_JFIELD_ORG("aType", 1) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("ip", req->ip) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("domain", domain) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("appName", req->app) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("tcUrl", req->tcUrl) << SRS_JFIELD_CONT  // Add tcUrl for auth publish rtmp stream client
        << SRS_JFIELD_STR("streamName", req->stream) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("time", time_ss.str()) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("param", req->param)
        << SRS_JOBJECT_END;
   */
    ss  << "aType=" << 1  
        << "&domain=" << domain
        << "&taskId=" << req->uuid
        << "&ip=" << req->client_ip
        << "&vhost=" << req->vhost
      	<< "&appName=" << req->app
	    << "&streamName=" << req->stream
	    << "&time=" << time_ss.str();
    
    std::string data = ss.str();
    std::string res;
    int status_code;
    if ((ret = do_post(url, data, status_code, res,http_header)) != ERROR_SUCCESS) {
        srs_collection("http post on_publish uri failed. "
            "client_id=%d, url=%s, request=%s, response=%s, code=%d, ret=%d",
            client_id, url.c_str(), data.c_str(), res.c_str(), status_code, ret);


        srs_error("http post on_publish uri failed. "
            "client_id=%d, url=%s, request=%s, response=%s, code=%d, ret=%d",
            client_id, url.c_str(), data.c_str(), res.c_str(), status_code, ret);
        return ret;
    }
    
    srs_collection("http hook on_publish success. "
        "client_id=%d, url=%s, request=%s, response=%s, ret=%d",
        client_id, url.c_str(), data.c_str(), res.c_str(), ret);


    srs_trace("http hook on_publish success. "
        "client_id=%d, url=%s, request=%s, response=%s, ret=%d",
        client_id, url.c_str(), data.c_str(), res.c_str(), ret);
    
    return ret;
}

int SrsHttpHooks::on_unpublish(string url, SrsRequest* req)
{
    int ret = ERROR_SUCCESS;
    
    int client_id = _srs_context->get_id();
    
    int64_t current_time = srs_update_system_time_ms();
    std::stringstream time_ss;
    time_ss << current_time;
 

    std::string domain = srs_get_param_key(req->param,"domain");    
    std::stringstream ss;

    std::string http_header="";
    
    SrsConfDirective* conf_key = _srs_config->get_vhost_http_header_key(req->vhost);
    SrsConfDirective* conf_value = _srs_config->get_vhost_http_header_value(req->vhost);
    if(conf_key && conf_value) {
        std::stringstream str_header;
        str_header << conf_key->args[0]
                   << ": "
                   << conf_value->args[0];
        http_header = str_header.str();
    }

    /*
    ss << SRS_JOBJECT_START
        << SRS_JFIELD_STR("action", "on_unpublish") << SRS_JFIELD_CONT
        << SRS_JFIELD_ORG("client_id", client_id) << SRS_JFIELD_CONT
        << SRS_JFIELD_ORG("aType", 2) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("ip", req->ip) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("domain", domain) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("appName", req->app) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("streamName", req->stream) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("time", time_ss.str()) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("param", req->param)
        << SRS_JOBJECT_END;
    */ 
    ss  << "aType=" << 2  
        << "&domain=" << domain
        << "&taskId=" << req->uuid
        << "&ip=" << req->client_ip
        << "&vhost=" << req->vhost
      	<< "&appName=" << req->app
	    << "&streamName=" << req->stream
	    << "&time=" << time_ss.str();
 
    std::string data = ss.str();
    std::string res;
    int status_code;
    if ((ret = do_post(url, data, status_code, res,http_header)) != ERROR_SUCCESS) {
        
        srs_collection("http post on_unpublish uri failed, ignored. "
            "client_id=%d, url=%s, request=%s, response=%s, code=%d, ret=%d",
            client_id, url.c_str(), data.c_str(), res.c_str(), status_code, ret);


        srs_warn("http post on_unpublish uri failed, ignored. "
            "client_id=%d, url=%s, request=%s, response=%s, code=%d, ret=%d",
            client_id, url.c_str(), data.c_str(), res.c_str(), status_code, ret);
        return ret;
    }
    
    srs_collection("http hook on_unpublish success. "
        "client_id=%d, url=%s, request=%s, response=%s, ret=%d",
        client_id, url.c_str(), data.c_str(), res.c_str(), ret);


    srs_trace("http hook on_unpublish success. "
        "client_id=%d, url=%s, request=%s, response=%s, ret=%d",
        client_id, url.c_str(), data.c_str(), res.c_str(), ret);
    
    return ret;
}

int SrsHttpHooks::on_unpublish(string url, string vhostname, string appname, string streamname)
{
    int ret = ERROR_SUCCESS;
    int64_t current_time = srs_update_system_time_ms();
    std::stringstream time_ss;
    time_ss << current_time;
    std::stringstream ss;

    std::string http_header="";
    SrsConfDirective* conf_key = _srs_config->get_vhost_http_header_key(vhostname);
    SrsConfDirective* conf_value = _srs_config->get_vhost_http_header_value(vhostname);
    if(conf_key && conf_value) {
        std::stringstream str_header;
        str_header << conf_key->args[0]
                   << ": "
                   << conf_value->args[0];
        http_header = str_header.str();
    }

    ss  << "aType=" << 2  
        //<< "&domain=" << domain
        //<< "&taskId=" << req->uuid
        //<< "&ip=" << req->client_ip
        << "&vhost=" << vhostname
      	<< "&appName=" << appname
	    << "&streamName=" << streamname
	    << "&time=" << time_ss.str();
 
    std::string data = ss.str();
    std::string res;
    int status_code;
    if ((ret = do_post(url, data, status_code, res, http_header)) != ERROR_SUCCESS) {
        srs_collection("http post on_unpublish uri failed, ignored. "
            "url=%s, request=%s, response=%s, code=%d, ret=%d",
            url.c_str(), data.c_str(), res.c_str(), status_code, ret);
        srs_warn("http post on_unpublish uri failed, ignored. "
            "url=%s, request=%s, response=%s, code=%d, ret=%d",
            url.c_str(), data.c_str(), res.c_str(), status_code, ret);
    } else {
		srs_collection("http hook on_unpublish success. "
			"url=%s, request=%s, response=%s, ret=%d",
			url.c_str(), data.c_str(), res.c_str(), ret);
		srs_trace("http hook on_unpublish success. "
			"url=%s, request=%s, response=%s, ret=%d",
			url.c_str(), data.c_str(), res.c_str(), ret);
	}
    return ret;
}



int SrsHttpHooks::on_play(string url, SrsRequest* req)
{
    int ret = ERROR_SUCCESS;
    
    int client_id = _srs_context->get_id();
    
    int64_t current_time = srs_update_system_time_ms();
    std::stringstream time_ss;
    time_ss << current_time;
 

    std::string domain = srs_get_param_key(req->param,"domain");    
    std::stringstream ss;

    std::string http_header="";
    
    SrsConfDirective* conf_key = _srs_config->get_vhost_http_header_key(req->vhost);
    SrsConfDirective* conf_value = _srs_config->get_vhost_http_header_value(req->vhost);
    if(conf_key && conf_value) {
        std::stringstream str_header;
        str_header << conf_key->args[0]
                   << ": "
                   << conf_value->args[0];
        http_header = str_header.str();
    }

    /*
    ss << SRS_JOBJECT_START
        << SRS_JFIELD_STR("action", "on_play") << SRS_JFIELD_CONT
        << SRS_JFIELD_ORG("client_id", client_id) << SRS_JFIELD_CONT
        << SRS_JFIELD_ORG("aType", 3) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("ip", req->ip) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("domain", domain) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("appName", req->app) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("streamName", req->stream) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("param", req->param) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("time", time_ss.str()) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("pageUrl", req->pageUrl)
        << SRS_JOBJECT_END;
    */ 
    ss  << "aType=" << 3 
        << "&domain=" << domain
        << "&vhost=" << req->vhost
      	<< "&appName=" << req->app
	    << "&streamName=" << req->stream
	    << "&time=" << time_ss.str();
 

 
    std::string data = ss.str();
    std::string res;
    int status_code;
    if ((ret = do_post(url, data, status_code, res,http_header)) != ERROR_SUCCESS) {
        srs_collection("http post on_play uri failed. "
            "client_id=%d, url=%s, request=%s, response=%s, code=%d, ret=%d",
            client_id, url.c_str(), data.c_str(), res.c_str(), status_code, ret);

        srs_error("http post on_play uri failed. "
            "client_id=%d, url=%s, request=%s, response=%s, code=%d, ret=%d",
            client_id, url.c_str(), data.c_str(), res.c_str(), status_code, ret);
        return ret;
    }
    
    srs_collection("http hook on_play success. "
        "client_id=%d, url=%s, request=%s, response=%s, ret=%d",
        client_id, url.c_str(), data.c_str(), res.c_str(), ret);


    srs_trace("http hook on_play success. "
        "client_id=%d, url=%s, request=%s, response=%s, ret=%d",
        client_id, url.c_str(), data.c_str(), res.c_str(), ret);
    
    return ret;
}

void SrsHttpHooks::on_recevie_transcode_stop(string url, SrsRequest* req)
{
    srs_collection("http hook on_recevie_stop success. ");
    srs_trace("http hook on_recevie_stop success. ");
    return;
}

void SrsHttpHooks::on_stop(string url, SrsRequest* req)
{
    int ret = ERROR_SUCCESS;
    
    int client_id = _srs_context->get_id();
    
    int64_t current_time = srs_update_system_time_ms();
    std::stringstream time_ss;
    time_ss << current_time;
 

    std::string domain = srs_get_param_key(req->param,"domain");    
    std::stringstream ss;
    
    std::string http_header="";
    
    SrsConfDirective* conf_key = _srs_config->get_vhost_http_header_key(req->vhost);
    SrsConfDirective* conf_value = _srs_config->get_vhost_http_header_value(req->vhost);
    if(conf_key && conf_value) {
        std::stringstream str_header;
        str_header << conf_key->args[0]
                   << ": "
                   << conf_value->args[0];
        http_header = str_header.str();
    }

    /*
    ss << SRS_JOBJECT_START
        << SRS_JFIELD_STR("action", "on_stop") << SRS_JFIELD_CONT
        << SRS_JFIELD_ORG("client_id", client_id) << SRS_JFIELD_CONT
        << SRS_JFIELD_ORG("aType", 4) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("ip", req->ip) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("domain", domain) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("appName", req->app) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("streamName", req->stream) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("time", time_ss.str()) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("param", req->param)
        << SRS_JOBJECT_END;
    */
    
    ss  << "aType=" << 4 
        << "&domain=" << domain
        << "&vhost=" << req->vhost
      	<< "&appName=" << req->app
	    << "&streamName=" << req->stream
	    << "&time=" << time_ss.str();
 
       
    std::string data = ss.str();
    std::string res;
    int status_code;
    if ((ret = do_post(url, data, status_code, res,http_header)) != ERROR_SUCCESS) {
        srs_collection("http post on_stop uri failed, ignored. "
            "client_id=%d, url=%s, request=%s, response=%s, code=%d, ret=%d",
            client_id, url.c_str(), data.c_str(), res.c_str(), status_code, ret);

        srs_warn("http post on_stop uri failed, ignored. "
            "client_id=%d, url=%s, request=%s, response=%s, code=%d, ret=%d",
            client_id, url.c_str(), data.c_str(), res.c_str(), status_code, ret);
        return;
    }

    srs_collection("http hook on_stop success. "
        "client_id=%d, url=%s, request=%s, response=%s, ret=%d",
        client_id, url.c_str(), data.c_str(), res.c_str(), ret);


    srs_trace("http hook on_stop success. "
        "client_id=%d, url=%s, request=%s, response=%s, ret=%d",
        client_id, url.c_str(), data.c_str(), res.c_str(), ret);
    
    return;
}

int SrsHttpHooks::on_dvr(int cid, string url, SrsRequest* req, string file)
{
    int ret = ERROR_SUCCESS;
    
    int client_id = cid;
    std::string cwd = _srs_config->cwd();
    
    std::stringstream ss;
    ss << SRS_JOBJECT_START
        << SRS_JFIELD_STR("action", "on_dvr") << SRS_JFIELD_CONT
        << SRS_JFIELD_ORG("client_id", client_id) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("ip", req->ip) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("app", req->app) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("stream", req->stream) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("param", req->param) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("cwd", cwd) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("file", file)
        << SRS_JOBJECT_END;
        
    std::string data = ss.str();
    std::string res;
    int status_code;
    if ((ret = do_post(url, data, status_code, res)) != ERROR_SUCCESS) {
        srs_error("http post on_dvr uri failed, ignored. "
            "client_id=%d, url=%s, request=%s, response=%s, code=%d, ret=%d",
            client_id, url.c_str(), data.c_str(), res.c_str(), status_code, ret);
        return ret;
    }
    
    srs_trace("http hook on_dvr success. "
        "client_id=%d, url=%s, request=%s, response=%s, ret=%d",
        client_id, url.c_str(), data.c_str(), res.c_str(), ret);
    
    return ret;
}

int SrsHttpHooks::on_hls(int cid, string url, SrsRequest* req, string file, string ts_url, string m3u8, string m3u8_url, int sn, double duration, int status)
{
    int ret = ERROR_SUCCESS;
    
    int client_id = cid;
    std::string cwd = _srs_config->cwd();
    
    // the ts_url is under the same dir of m3u8_url.
    string prefix = srs_path_dirname(m3u8_url);
    if (!prefix.empty() && !srs_string_is_http(ts_url)) {
        ts_url = prefix + "/" + ts_url;
    }
	/*
    std::string accesskey = _srs_config->get_upload_accesskey();
    std::string secretkey = _srs_config->get_upload_secretkey();
    std::string bucketname = _srs_config->get_upload_bucketname();
	*/
    //std::string callbackurl = _srs_config->get_upload_callbackurl();
    
    /*
    int filetype = 0;
    
    if (srs_string_ends_with(file,".m3u8")) {
        filetype = 1;
    }
    */

	/* RD: update record info */
	if (_srs_config->get_scheduler_enabled()) {
		std::string url = _srs_config->get_scheduler_url();
		if ((ret = SrsHttpHooks::on_scheduler_record_update(url, req, file)) != ERROR_SUCCESS) {
			srs_warn("record update failded!");
			return ret;
		}
	}

	std::stringstream ss;
	std::string domain = srs_get_param_key(req->param, "domain");
	// upload ts and m3u8
    ss << SRS_JOBJECT_START
        << SRS_JFIELD_STR("action", "") << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("taskId", req->uuid) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("domain", req->domain) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("appName", req->app) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("streamName", req->stream) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("tsPath", file) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("m3u8Path", m3u8) << SRS_JFIELD_CONT
        << SRS_JFIELD_ORG("streamStatus", status) 
        //<< SRS_JFIELD_CONT
        //<< SRS_JFIELD_ORG("fileType", filetype) << SRS_JFIELD_CONT
        //<< SRS_JFIELD_ORG("srcType", 1) << SRS_JFIELD_CONT
        //<< SRS_JFIELD_ORG("desType", 1) << SRS_JFIELD_CONT
        //<< SRS_JFIELD_BOOL("m3u8", ism3u8) << SRS_JFIELD_CONT
        //<< SRS_JFIELD_STR("callbackUrl", callbackurl) 
        << SRS_JOBJECT_END;
        
    std::string data = ss.str();
    std::string res;
    int status_code;
    if ((ret = do_post(url, data, status_code, res, "", true)) != ERROR_SUCCESS) {
        srs_error("http post on_hls uri failed, ignored. "
            "client_id=%d, url=%s, request=%s, response=%s, code=%d, ret=%d",
            client_id, url.c_str(), data.c_str(), res.c_str(), status_code, ret);
        return ret;
    }
    srs_trace("http hook on_hls success. "
        "client_id=%d, url=%s, request=%s, response=%s, ret=%d",
        client_id, url.c_str(), data.c_str(), res.c_str(), ret);

    return ret;
}

int SrsHttpHooks::on_hls_notify(int cid, std::string url, SrsRequest* req, std::string ts_url, int nb_notify)
{
    int ret = ERROR_SUCCESS;
    
    int client_id = cid;
    std::string cwd = _srs_config->cwd();
    
    if (srs_string_is_http(ts_url)) {
        url = ts_url;
    }
    
    url = srs_string_replace(url, "[app]", req->app);
    url = srs_string_replace(url, "[stream]", req->stream);
    url = srs_string_replace(url, "[ts_url]", ts_url);
    url = srs_string_replace(url, "[param]", req->param);
    
    int64_t starttime = srs_update_system_time_ms();
    
    SrsHttpUri uri;
    if ((ret = uri.initialize(url)) != ERROR_SUCCESS) {
        srs_error("http: post failed. url=%s, ret=%d", url.c_str(), ret);
        return ret;
    }
    
    SrsHttpClient http;
    if ((ret = http.initialize(uri.get_host(), uri.get_port(), SRS_HLS_NOTIFY_TIMEOUT_US)) != ERROR_SUCCESS) {
        return ret;
    }
    
    std::string path = uri.get_query();
    if (path.empty()) {
        path = uri.get_path();
    } else {
        path = uri.get_path();
        path += "?";
        path += uri.get_query();
    }
    srs_warn("GET %s", path.c_str());
    
    ISrsHttpMessage* msg = NULL;
    if ((ret = http.get(path.c_str(), "", &msg)) != ERROR_SUCCESS) {
        return ret;
    }
    SrsAutoFree(ISrsHttpMessage, msg);
    
    int nb_buf = srs_min(nb_notify, SRS_HTTP_READ_BUFFER);
    char* buf = new char[nb_buf];
    SrsAutoFreeA(char, buf);
    
    int nb_read = 0;
    ISrsHttpResponseReader* br = msg->body_reader();
    while (nb_read < nb_notify && !br->eof()) {
        int nb_bytes = 0;
        if ((ret = br->read(buf, nb_buf, &nb_bytes)) != ERROR_SUCCESS) {
            break;
        }
        nb_read += nb_bytes;
    }
    
    int spenttime = (int)(srs_update_system_time_ms() - starttime);
    srs_trace("http hook on_hls_notify success. client_id=%d, url=%s, code=%d, spent=%dms, read=%dB, ret=%d",
        client_id, url.c_str(), msg->status_code(), spenttime, nb_read, ret);
    
    // ignore any error for on_hls_notify.
    ret = ERROR_SUCCESS;
    
    return ret;
}

int SrsHttpHooks::on_timestamp_jump_notify(std::string url, SrsRequest* req, int64_t start_time, int64_t end_time) {
    
    int ret = ERROR_SUCCESS;
    
    int client_id = _srs_context->get_id();
   
    int64_t current_time = srs_update_system_time_ms();
    std::stringstream time_ss;
    time_ss << current_time;
 
    std::string domain = srs_get_param_key(req->param,"domain");    
    std::stringstream ss;
   
    std::string http_header="";
    
    SrsConfDirective* conf_key = _srs_config->get_vhost_http_header_key(req->vhost);
    SrsConfDirective* conf_value = _srs_config->get_vhost_http_header_value(req->vhost);
    if(conf_key && conf_value) {
        std::stringstream str_header;
        str_header << conf_key->args[0]
                   << ": "
                   << conf_value->args[0];
        http_header = str_header.str();
    }
    
    ss  << "domain=" << domain
        << "&taskId=" << req->uuid
      	<< "&appName=" << req->app
	    << "&streamName=" << req->stream
	    << "&startTime=" << start_time
        << "&endTime=" << end_time;
    
    std::string data = ss.str();
    std::string res;
    int status_code;
    if ((ret = do_post2(url, data, status_code, res,http_header)) != ERROR_SUCCESS) {
        srs_collection("http post on_timestamp_jump_notify uri failed. "
            "client_id=%d, url=%s, request=%s, response=%s, code=%d, ret=%d",
            client_id, url.c_str(), data.c_str(), res.c_str(), status_code, ret);


        srs_error("http post on_timestamp_jump_notify uri failed. "
            "client_id=%d, url=%s, request=%s, response=%s, code=%d, ret=%d",
            client_id, url.c_str(), data.c_str(), res.c_str(), status_code, ret);
        return ret;
    }
    
    srs_collection("http hook on_timestamp_jump_notify success. "
        "client_id=%d, url=%s, request=%s, response=%s, ret=%d",
        client_id, url.c_str(), data.c_str(), res.c_str(), ret);


    srs_trace("http hook on_timestamp_jump_notify success. "
        "client_id=%d, url=%s, request=%s, response=%s, ret=%d",
        client_id, url.c_str(), data.c_str(), res.c_str(), ret);
    
    return ret;

}

int SrsHttpHooks::do_post(std::string url, std::string req, int& code, string& res,const std::string http_header ,bool is_json)
{
    int ret = ERROR_SUCCESS;
    
    SrsHttpUri uri;
    if ((ret = uri.initialize(url)) != ERROR_SUCCESS) {
        srs_error("http: post failed. url=%s, ret=%d", url.c_str(), ret);
        return ret;
    }
    
    SrsHttpClient http;
    if ((ret = http.initialize(uri.get_host(), uri.get_port())) != ERROR_SUCCESS) {
        return ret;
    }
    
    ISrsHttpMessage* msg = NULL;
    if ((ret = http.post(uri.get_path(), req, &msg,http_header,is_json)) != ERROR_SUCCESS) {
        return ret;
    }
    SrsAutoFree(ISrsHttpMessage, msg);
    
    code = msg->status_code();
    if ((ret = msg->body_read_all(res)) != ERROR_SUCCESS) {
        return ret;
    }
    
    // ensure the http status is ok.
    // https://github.com/ossrs/srs/issues/158
    if (code != SRS_CONSTS_HTTP_OK) {
        ret = ERROR_HTTP_STATUS_INVALID;
        srs_error("invalid response status=%d. ret=%d", code, ret);
        return ret;
    }
    
    // should never be empty.
    if (res.empty()) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid empty response. ret=%d", ret);
        return ret;
    }

    srs_collection("http response: %s ",res.c_str());
    // parse string res to json.
    //std::string json = "{\"status\":1,\"code\":\"0001\",\"data\":{\"status\":0} }";
    //std::string json = "{\"timestamp\":1566446290225,\"status\":400,\"error\":\"Bad Request\",\"message\":\"Required String parameter 'domain' is not present\",\"path\":\"/live/api/inner/stream/callback\"}";
    SrsJsonAny* info = SrsJsonAny::loads((char*)res.c_str());
    if (!info) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid response %s. ret=%d", res.c_str(), ret);
        return ret;
    }
    SrsAutoFree(SrsJsonAny, info);
    
    
    // response error code in string.
    if (!info->is_object()) {
        if (res != SRS_HTTP_RESPONSE_OK) {
            ret = ERROR_HTTP_DATA_INVALID;
            srs_error("invalid response number %s. ret=%d", res.c_str(), ret);
            return ret;
        }
        return ret;
    }
    
    // response standard object, format in json: {"status": 0, "data": ""}
    SrsJsonObject* res_info = info->to_object();
    SrsJsonAny* res_code = NULL;
    if ((res_code = res_info->get_property("data")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
    
    SrsJsonObject* data  = NULL;
    if (!res_code->is_object() || (data = res_code->to_object()) == NULL ) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    } 
    SrsJsonAny* status_code = NULL;
    if ((status_code = data->ensure_property_integer("status")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
    if ((status_code->to_integer()) != ERROR_SUCCESS) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("error response code=%d. ret=%d", status_code->to_integer(), ret);
        return ret;
    }

    return ret;
}

int SrsHttpHooks::do_post2(std::string url, std::string req, int& code, string& res,const std::string http_header )
{
    int ret = ERROR_SUCCESS;
    
    SrsHttpUri uri;
    if ((ret = uri.initialize(url)) != ERROR_SUCCESS) {
        srs_error("http: post failed. url=%s, ret=%d", url.c_str(), ret);
        return ret;
    }
    
    SrsHttpClient http;
    if ((ret = http.initialize(uri.get_host(), uri.get_port())) != ERROR_SUCCESS) {
        return ret;
    }
    
    ISrsHttpMessage* msg = NULL;
    if ((ret = http.post(uri.get_path(), req, &msg,http_header)) != ERROR_SUCCESS) {
        return ret;
    }
    SrsAutoFree(ISrsHttpMessage, msg);
    
    code = msg->status_code();
    if ((ret = msg->body_read_all(res)) != ERROR_SUCCESS) {
        return ret;
    }
    
    // ensure the http status is ok.
    // https://github.com/ossrs/srs/issues/158
    if (code != SRS_CONSTS_HTTP_OK) {
        ret = ERROR_HTTP_STATUS_INVALID;
        srs_error("invalid response status=%d. ret=%d", code, ret);
        return ret;
    }
    
    // should never be empty.
    if (res.empty()) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid empty response. ret=%d", ret);
        return ret;
    }

    srs_collection("http response: %s ",res.c_str());
    // parse string res to json.
    //std::string json = "{\"status\":1,\"code\":\"0001\",\"data\":{\"status\":0} }";
    //std::string json = "{\"timestamp\":1566446290225,\"status\":400,\"error\":\"Bad Request\",\"message\":\"Required String parameter 'domain' is not present\",\"path\":\"/live/api/inner/stream/callback\"}";
    SrsJsonAny* info = SrsJsonAny::loads((char*)res.c_str());
    if (!info) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid response %s. ret=%d", res.c_str(), ret);
        return ret;
    }
    SrsAutoFree(SrsJsonAny, info);
    
    
    // response error code in string.
    if (!info->is_object()) {
        if (res != SRS_HTTP_RESPONSE_OK) {
            ret = ERROR_HTTP_DATA_INVALID;
            srs_error("invalid response number %s. ret=%d", res.c_str(), ret);
            return ret;
        }
        return ret;
    }
    
    // response standard object, format in json: {"status": 0, "data": ""}
    SrsJsonObject* res_info = info->to_object();
    
    SrsJsonAny* status_code = NULL;
    if ((status_code = res_info->ensure_property_integer("status")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }

    if ((status_code->to_integer()) != 1) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("error response code=%d. ret=%d", status_code->to_integer(), ret);
        return ret;
    }
    
    return ret;
}

int SrsHttpHooks::on_notify_upload_recover(std::string url, SrsRequest* req, std::string path) 
{
    int ret = ERROR_SUCCESS;
    int code;
    string res;
    std::stringstream ss;
	
    ss << SRS_JOBJECT_START
        << SRS_JFIELD_STR("path", path) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("domain", req->domain) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("appName", req->app) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("streamName", req->stream)
        << SRS_JOBJECT_END;
    
    SrsHttpUri uri;
    if ((ret = uri.initialize(url)) != ERROR_SUCCESS) {
        srs_error("http: post failed. url=%s, ret=%d", url.c_str(), ret);
        return ret;
    }
    
    SrsHttpClient http;
    if ((ret = http.initialize(uri.get_host(), uri.get_port())) != ERROR_SUCCESS) {
        return ret;
    }
    
    ISrsHttpMessage* msg = NULL;
    if ((ret = http.post(uri.get_path(), ss.str(), &msg, "", true)) != ERROR_SUCCESS) {
        return ret;
    }
    SrsAutoFree(ISrsHttpMessage, msg);
    
    code = msg->status_code();
    if ((ret = msg->body_read_all(res)) != ERROR_SUCCESS) {
        return ret;
    }
    if (code != SRS_CONSTS_HTTP_OK) {
	    ret = ERROR_HTTP_STATUS_INVALID;
	    srs_error("invalid response status=%d. ret=%d", code, ret);
	    return ret;
    }
    
    // should never be empty.
    if (res.empty()) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid empty response. ret=%d", ret);
        return ret;
    }

    srs_collection("http response: %s ",res.c_str());

    // parse string res to json.
    SrsJsonAny* info = SrsJsonAny::loads((char*)res.c_str());
    if (!info) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid response %s. ret=%d", res.c_str(), ret);
        return ret;
    }
    SrsAutoFree(SrsJsonAny, info);

	// response error code in string.
	if (!info->is_object()) {
		if (res != SRS_HTTP_RESPONSE_OK) {
			ret = ERROR_HTTP_DATA_INVALID;
			srs_error("invalid response number %s. ret=%d", res.c_str(), ret);
			return ret;
		}
		return ret;
	}

    return ret;
}

int SrsHttpHooks::on_notify_scheduler_play(std::string url,SrsRequest* req,std::string& ip,std::string& port) {
    
        int ret = ERROR_SUCCESS;
        int code;
        string res;
        std::stringstream ss;
		std::string domain = srs_get_param_key(req->param,"domain");	
		int64_t current_time = srs_get_system_time_ms();
		std::stringstream time_ss;
		time_ss << current_time;
		
        ss << SRS_JOBJECT_START
            << SRS_JFIELD_STR("action", "on_play") << SRS_JFIELD_CONT
            << SRS_JFIELD_STR("domain", domain) << SRS_JFIELD_CONT
            << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
            << SRS_JFIELD_STR("appName", req->app) << SRS_JFIELD_CONT
            << SRS_JFIELD_STR("streamName", req->stream) << SRS_JFIELD_CONT
            << SRS_JFIELD_STR("time", time_ss.str())
            << SRS_JOBJECT_END;
        
        SrsHttpUri uri;
        if ((ret = uri.initialize(url)) != ERROR_SUCCESS) {
            srs_error("http: post failed. url=%s, ret=%d", url.c_str(), ret);
            return ret;
        }
        
        SrsHttpClient http;
        if ((ret = http.initialize(uri.get_host(), uri.get_port())) != ERROR_SUCCESS) {
            return ret;
        }
        
        ISrsHttpMessage* msg = NULL;
        if ((ret = http.post(uri.get_path(), ss.str(), &msg,"",true)) != ERROR_SUCCESS) {
            return ret;
        }
        SrsAutoFree(ISrsHttpMessage, msg);
        
        code = msg->status_code();
        if ((ret = msg->body_read_all(res)) != ERROR_SUCCESS) {
            return ret;
        }
        if (code != SRS_CONSTS_HTTP_OK) {
        ret = ERROR_HTTP_STATUS_INVALID;
        srs_error("invalid response status=%d. ret=%d", code, ret);
        return ret;
    }
    
    // should never be empty.
    if (res.empty()) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid empty response. ret=%d", ret);
        return ret;
    }

    srs_collection("http response: %s ",res.c_str());
    // parse string res to json.
    //std::string json = "{\"status\":1,\"code\":\"0001\",\"data\":{\"status\":0} }";
    //std::string json = "{\"timestamp\":1566446290225,\"status\":400,\"error\":\"Bad Request\",\"message\":\"Required String parameter 'domain' is not present\",\"path\":\"/live/api/inner/stream/callback\"}";
    SrsJsonAny* info = SrsJsonAny::loads((char*)res.c_str());
    if (!info) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid response %s. ret=%d", res.c_str(), ret);
        return ret;
    }
    SrsAutoFree(SrsJsonAny, info);
    
    
    // response error code in string.
    if (!info->is_object()) {
        if (res != SRS_HTTP_RESPONSE_OK) {
            ret = ERROR_HTTP_DATA_INVALID;
            srs_error("invalid response number %s. ret=%d", res.c_str(), ret);
            return ret;
        }
        return ret;
    }
    
    // response standard object, format in json: {"status": 0, "data": ""}
    SrsJsonObject* res_info = info->to_object();
        
    SrsJsonAny* status_code = NULL;
    if ((status_code = res_info->ensure_property_integer("status")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
    if ((status_code->to_integer()) != ERROR_SUCCESS) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("error response code=%d. ret=%d", status_code->to_integer(), ret);
        return ret;
    }

    SrsJsonAny* ip_info = NULL;
    if ((ip_info = res_info->ensure_property_string("ip")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
        
    SrsJsonAny* port_info = NULL;
    if ((port_info = res_info->ensure_property_string("port")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
    
    ip = ip_info->to_str();
    port = port_info->to_str();
    return ret;
}

int SrsHttpHooks::on_notify_scheduler_publish(std::string url, SrsRequest* req, std::string& ip, std::string& port) {
    
    int ret = ERROR_SUCCESS;
    int code;
    string res;
    std::stringstream ss;
    std::vector<std::string> ip_ports = _srs_config->get_listens();
    srs_assert((int)ip_ports.size() > 0);


    std::string local_ip;
    std::string local_port;
    for (int i = 0; i < (int)ip_ports.size(); i++) {
        srs_parse_endpoint(ip_ports[i], local_ip, local_port);
    }

	std::string cluster_id = _srs_config->get_cluster_clusterid();        
    std::vector<std::string> ips = srs_get_local_ipv4_ips();
    assert(_srs_config->get_stats_network() < (int)ips.size());
    local_ip = ips[_srs_config->get_stats_network()];

    ss << SRS_JOBJECT_START
        << SRS_JFIELD_STR("action", "on_publish") << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("domain", req->domain) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("appName", req->app) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("streamName", req->stream) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("cluster_id", cluster_id) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("ip", local_ip) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("port", local_port) 
        << SRS_JOBJECT_END;
    
    SrsHttpUri uri;
    if ((ret = uri.initialize(url)) != ERROR_SUCCESS) {
        srs_error("http: post failed. url=%s, ret=%d", url.c_str(), ret);
        return ret;
    }
    
    SrsHttpClient http;
    if ((ret = http.initialize(uri.get_host(), uri.get_port())) != ERROR_SUCCESS) {
        return ret;
    }
    
    ISrsHttpMessage* msg = NULL;
    if ((ret = http.post(uri.get_path(), ss.str(), &msg,"",true)) != ERROR_SUCCESS) {
        return ret;
    }
    SrsAutoFree(ISrsHttpMessage, msg);
    
    code = msg->status_code();
    if ((ret = msg->body_read_all(res)) != ERROR_SUCCESS) {
        return ret;
    }
    if (code != SRS_CONSTS_HTTP_OK) {
    ret = ERROR_HTTP_STATUS_INVALID;
    srs_error("invalid response status=%d. ret=%d", code, ret);
    return ret;
    }
    
    // should never be empty.
    if (res.empty()) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid empty response. ret=%d", ret);
        return ret;
    }

    srs_collection("http response: %s ",res.c_str());
    // parse string res to json.
    //std::string json = "{\"status\":1,\"code\":\"0001\",\"data\":{\"status\":0} }";
    //std::string json = "{\"timestamp\":1566446290225,\"status\":400,\"error\":\"Bad Request\",\"message\":\"Required String parameter 'domain' is not present\",\"path\":\"/live/api/inner/stream/callback\"}";
    SrsJsonAny* info = SrsJsonAny::loads((char*)res.c_str());
    if (!info) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid response %s. ret=%d", res.c_str(), ret);
        return ret;
    }
    SrsAutoFree(SrsJsonAny, info);
    
    
    // response error code in string.
    if (!info->is_object()) {
        if (res != SRS_HTTP_RESPONSE_OK) {
            ret = ERROR_HTTP_DATA_INVALID;
            srs_error("invalid response number %s. ret=%d", res.c_str(), ret);
            return ret;
        }
        return ret;
    }
    
    // response standard object, format in json: {"status": 0, "data": ""}
    SrsJsonObject* res_info = info->to_object();
        
    SrsJsonAny* status_code = NULL;
    if ((status_code = res_info->ensure_property_integer("status")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
    if ((status_code->to_integer()) != ERROR_SUCCESS) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("error response code=%d. ret=%d", status_code->to_integer(), ret);
        return ret;
    }
    SrsJsonAny* ip_info = NULL;
    if ((ip_info = res_info->ensure_property_string("ip")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
        
    SrsJsonAny* port_info = NULL;
    if ((port_info = res_info->ensure_property_string("port")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
        
    SrsJsonAny* recover_flag = NULL;
	if ((recover_flag = res_info->ensure_property_string("recover_flag")) == NULL) {
		req->recover_flag = false;
	} else {
		req->recover_flag = true;
	}
    ip = ip_info->to_str();
    port = port_info->to_str();


    return ret;
}

int SrsHttpHooks::on_notify_scheduler_republish(std::string url,SrsRequest* req, std::string& exclude_ip, std::string& exclude_port, std::string& ip, std::string& port) {
    
    int ret = ERROR_SUCCESS;
    int code;
    string res;
    std::stringstream ss;
    std::vector<std::string> ip_ports = _srs_config->get_listens();
    srs_assert((int)ip_ports.size() > 0);


    std::string local_ip;
    std::string local_port;
    for (int i = 0; i < (int)ip_ports.size(); i++) {
        srs_parse_endpoint(ip_ports[i], local_ip, local_port);
    }

	std::string cluster_id = _srs_config->get_cluster_clusterid();        
    std::vector<std::string> ips = srs_get_local_ipv4_ips();
    assert(_srs_config->get_stats_network() < (int)ips.size());
    local_ip = ips[_srs_config->get_stats_network()];

    ss << SRS_JOBJECT_START
        << SRS_JFIELD_STR("action", "on_republish") << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("appName", req->app) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("streamName", req->stream) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("cluster_id", cluster_id) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("ip", local_ip) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("port", local_port) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("exclude_ip", exclude_ip) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("exclude_port", exclude_port) 
        << SRS_JOBJECT_END;
    
    SrsHttpUri uri;
    if ((ret = uri.initialize(url)) != ERROR_SUCCESS) {
        srs_error("http: post failed. url=%s, ret=%d", url.c_str(), ret);
        return ret;
    }
    
    SrsHttpClient http;
    if ((ret = http.initialize(uri.get_host(), uri.get_port())) != ERROR_SUCCESS) {
        return ret;
    }
    
    ISrsHttpMessage* msg = NULL;
    if ((ret = http.post(uri.get_path(), ss.str(), &msg,"",true)) != ERROR_SUCCESS) {
        return ret;
    }
    SrsAutoFree(ISrsHttpMessage, msg);
    
    code = msg->status_code();
    if ((ret = msg->body_read_all(res)) != ERROR_SUCCESS) {
        return ret;
    }
    if (code != SRS_CONSTS_HTTP_OK) {
	    ret = ERROR_HTTP_STATUS_INVALID;
	    srs_error("invalid response status=%d. ret=%d", code, ret);
	    return ret;
    }
    
    // should never be empty.
    if (res.empty()) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid empty response. ret=%d", ret);
        return ret;
    }

    srs_collection("http response: %s ",res.c_str());
    // parse string res to json.
    //std::string json = "{\"status\":1,\"code\":\"0001\",\"data\":{\"status\":0} }";
    //std::string json = "{\"timestamp\":1566446290225,\"status\":400,\"error\":\"Bad Request\",\"message\":\"Required String parameter 'domain' is not present\",\"path\":\"/live/api/inner/stream/callback\"}";
    SrsJsonAny* info = SrsJsonAny::loads((char*)res.c_str());
    if (!info) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid response %s. ret=%d", res.c_str(), ret);
        return ret;
    }
    SrsAutoFree(SrsJsonAny, info);
    
    // response error code in string.
    if (!info->is_object()) {
        if (res != SRS_HTTP_RESPONSE_OK) {
            ret = ERROR_HTTP_DATA_INVALID;
            srs_error("invalid response number %s. ret=%d", res.c_str(), ret);
            return ret;
        }
        return ret;
    }
    
    // response standard object, format in json: {"status": 0, "data": ""}
    SrsJsonObject* res_info = info->to_object();
        
    SrsJsonAny* status_code = NULL;
    if ((status_code = res_info->ensure_property_integer("status")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
    if ((status_code->to_integer()) != ERROR_SUCCESS) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("error response code=%d. ret=%d", status_code->to_integer(), ret);
        return ret;
    }
    SrsJsonAny* ip_info = NULL;
    if ((ip_info = res_info->ensure_property_string("ip")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
        
    SrsJsonAny* port_info = NULL;
    if ((port_info = res_info->ensure_property_string("port")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
    
    ip = ip_info->to_str();
    port = port_info->to_str();
    return ret;
}


int SrsHttpHooks::on_notify_scheduler_unpublish(std::string url,SrsRequest* req) {
    
        int ret = ERROR_SUCCESS;
        int code;
        string res;
        std::stringstream ss;
        std::vector<std::string> ip_ports = _srs_config->get_listens();
        srs_assert((int)ip_ports.size() > 0);
    
    
        std::string local_ip;
        std::string local_port;
        for (int i = 0; i < (int)ip_ports.size(); i++) {
            srs_parse_endpoint(ip_ports[i], local_ip, local_port);
        }
        
		std::string cluster_id = _srs_config->get_cluster_clusterid();        
        std::vector<std::string> ips = srs_get_local_ipv4_ips();
        assert(_srs_config->get_stats_network() < (int)ips.size());
        local_ip = ips[_srs_config->get_stats_network()];

        ss << SRS_JOBJECT_START
            << SRS_JFIELD_STR("action", "on_unpublish") << SRS_JFIELD_CONT
            << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
            << SRS_JFIELD_STR("appName", req->app) << SRS_JFIELD_CONT
            << SRS_JFIELD_STR("streamName", req->stream) << SRS_JFIELD_CONT
            << SRS_JFIELD_STR("cluster_id", cluster_id) << SRS_JFIELD_CONT
            << SRS_JFIELD_STR("ip", local_ip) << SRS_JFIELD_CONT
            << SRS_JFIELD_STR("port", local_port) 
            << SRS_JOBJECT_END;
        
        SrsHttpUri uri;
        if ((ret = uri.initialize(url)) != ERROR_SUCCESS) {
            srs_error("http: post failed. url=%s, ret=%d", url.c_str(), ret);
            return ret;
        }
        
        SrsHttpClient http;
        if ((ret = http.initialize(uri.get_host(), uri.get_port())) != ERROR_SUCCESS) {
            return ret;
        }
        
        ISrsHttpMessage* msg = NULL;
        if ((ret = http.post(uri.get_path(), ss.str(), &msg,"",true)) != ERROR_SUCCESS) {
            return ret;
        }
        SrsAutoFree(ISrsHttpMessage, msg);
        
        code = msg->status_code();
        if ((ret = msg->body_read_all(res)) != ERROR_SUCCESS) {
            return ret;
        }
        if (code != SRS_CONSTS_HTTP_OK) {
            ret = ERROR_HTTP_STATUS_INVALID;
            srs_error("invalid response status=%d. ret=%d", code, ret);
            return ret;
        }
    
    // should never be empty.
    if (res.empty()) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid empty response. ret=%d", ret);
        return ret;
    }

    srs_collection("http response: %s ",res.c_str());
    // parse string res to json.
    //std::string json = "{\"status\":1,\"code\":\"0001\",\"data\":{\"status\":0} }";
    //std::string json = "{\"timestamp\":1566446290225,\"status\":400,\"error\":\"Bad Request\",\"message\":\"Required String parameter 'domain' is not present\",\"path\":\"/live/api/inner/stream/callback\"}";
    SrsJsonAny* info = SrsJsonAny::loads((char*)res.c_str());
    if (!info) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid response %s. ret=%d", res.c_str(), ret);
        return ret;
    }
    SrsAutoFree(SrsJsonAny, info);
    
    
    // response error code in string.
    if (!info->is_object()) {
        if (res != SRS_HTTP_RESPONSE_OK) {
            ret = ERROR_HTTP_DATA_INVALID;
            srs_error("invalid response number %s. ret=%d", res.c_str(), ret);
            return ret;
        }
        return ret;
    }
    
    // response standard object, format in json: {"status": 0, "data": ""}
    SrsJsonObject* res_info = info->to_object();
        
    SrsJsonAny* status_code = NULL;
    if ((status_code = res_info->ensure_property_integer("status")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
    if ((status_code->to_integer()) != ERROR_SUCCESS) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("error response code=%d. ret=%d", status_code->to_integer(), ret);
        return ret;
    }
    return ret;
}

int SrsHttpHooks::on_check(std::string url,SrsRequest* req) 
{
	int ret = ERROR_SUCCESS;
	int code;
	string res;
	std::stringstream ss;
	std::vector<std::string> ip_ports = _srs_config->get_listens();
	srs_assert((int)ip_ports.size() > 0);

	std::string local_ip;
	std::string local_port;
	for (int i = 0; i < (int)ip_ports.size(); i++) {
		srs_parse_endpoint(ip_ports[i], local_ip, local_port);
	}

	std::string cluster_id = _srs_config->get_cluster_clusterid();
	std::vector<std::string> ips = srs_get_local_ipv4_ips();
	assert(_srs_config->get_stats_network() < (int)ips.size());
	local_ip = ips[_srs_config->get_stats_network()];

	ss << SRS_JOBJECT_START
		<< SRS_JFIELD_STR("action", "on_check") << SRS_JFIELD_CONT
		<< SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
		<< SRS_JFIELD_STR("appName", req->app) << SRS_JFIELD_CONT
		<< SRS_JFIELD_STR("streamName", req->stream) << SRS_JFIELD_CONT
		<< SRS_JFIELD_STR("cluster_id", cluster_id)
		<< SRS_JOBJECT_END;

	SrsHttpUri uri;
	if ((ret = uri.initialize(url)) != ERROR_SUCCESS) {
		srs_error("http post failed. url=%s, ret=%d", url.c_str(), ret);
		return ret;
	}

	SrsHttpClient http;
	if ((ret = http.initialize(uri.get_host(), uri.get_port())) != ERROR_SUCCESS) {
		srs_error("http post failed. url=%s, ret=%d", url.c_str(), ret);
		return ret;
	}

	ISrsHttpMessage* msg = NULL;
	if ((ret = http.post(uri.get_path(), ss.str(), &msg, "", true)) != ERROR_SUCCESS) {
		srs_error("http post failed. url=%s, ret=%d", url.c_str(), ret);
		return ret;
	}
	SrsAutoFree(ISrsHttpMessage, msg);

	code = msg->status_code();
	if ((ret = msg->body_read_all(res)) != ERROR_SUCCESS) {
		return ret;
	}
	if (code != SRS_CONSTS_HTTP_OK) {
		ret = ERROR_HTTP_STATUS_INVALID;
		srs_error("invalid response status=%d, ret=%d", code, ret);
	}

	// should never empty
	if (res.empty()) {
		ret = ERROR_HTTP_DATA_INVALID;
		srs_error("invalid empty respinse. ret=%d", ret);
		return ret;
	}

	srs_collection("http response:%s", res.c_str());
	SrsJsonAny* info = SrsJsonAny::loads((char*)res.c_str());
	if (!info) {
		ret = ERROR_HTTP_DATA_INVALID;
		srs_error("invalid response:%s, ret=%d", res.c_str(), ret);
		return ret;
	}
	SrsAutoFree(SrsJsonAny, info);

	if (!info->is_object()) {
		if (ret != ERROR_SUCCESS) { //  SRS_HTTP_RESPONSE_OK
			ret = ERROR_HTTP_DATA_INVALID;
			srs_error("invalid response number:%s, ret=%d", res.c_str(), ret);
			return ret;
		}
		return ret;
	}

	SrsJsonObject* res_info = info->to_object();

	SrsJsonAny* status_code = NULL;
	if ((status_code = res_info->ensure_property_integer("status")) == NULL) {
		ret = ERROR_RESPONSE_CODE;
		srs_error("invalid response without code, ret=%d", ret);
		return ret;
	}
	if (status_code->to_integer() != ERROR_SUCCESS) {
		ret = ERROR_RESPONSE_CODE;
		srs_error("error response code=%d, ret=%d", status_code->to_integer(), ret);
		return ret;
	}
    return ret;
}

int SrsHttpHooks::on_scheduler_alloc_record_node(std::string url, SrsRequest* req, std::string& ip, std::string& port) 
{
    int ret = ERROR_SUCCESS;
    int code;
    string res;
    std::stringstream ss;
    std::vector<std::string> ip_ports = _srs_config->get_listens();
    srs_assert((int)ip_ports.size() > 0);

    std::string local_ip;
    std::string local_port;
    for (int i = 0; i < (int)ip_ports.size(); i++) {
        srs_parse_endpoint(ip_ports[i], local_ip, local_port);
    }

	std::string cluster_id = _srs_config->get_cluster_clusterid();        
    std::vector<std::string> ips = srs_get_local_ipv4_ips();
    assert(_srs_config->get_stats_network() < (int)ips.size());
    local_ip = ips[_srs_config->get_stats_network()];

    ss << SRS_JOBJECT_START
        << SRS_JFIELD_STR("action", "AllocRecordNode") << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("domain", req->domain) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("appName", req->app) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("streamName", req->stream) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("cluster_id", cluster_id) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("ip", local_ip) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("port", local_port) 
        << SRS_JOBJECT_END;
    
    SrsHttpUri uri;
    if ((ret = uri.initialize(url)) != ERROR_SUCCESS) {
        srs_error("http: post failed. url=%s, ret=%d", url.c_str(), ret);
        return ret;
    }
    
    SrsHttpClient http;
    if ((ret = http.initialize(uri.get_host(), uri.get_port())) != ERROR_SUCCESS) {
        return ret;
    }
    
    ISrsHttpMessage* msg = NULL;
    if ((ret = http.post(uri.get_path(), ss.str(), &msg,"",true)) != ERROR_SUCCESS) {
        return ret;
    }
    SrsAutoFree(ISrsHttpMessage, msg);
    
    code = msg->status_code();
    if ((ret = msg->body_read_all(res)) != ERROR_SUCCESS) {
        return ret;
    }
    if (code != SRS_CONSTS_HTTP_OK) {
    ret = ERROR_HTTP_STATUS_INVALID;
    srs_error("invalid response status=%d. ret=%d", code, ret);
    return ret;
    }
    
    // should never be empty.
    if (res.empty()) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid empty response. ret=%d", ret);
        return ret;
    }
    srs_collection("http response: %s ",res.c_str());
    
    // parse string res to json.
    //std::string json = "{\"status\":1,\"code\":\"0001\",\"data\":{\"status\":0} }";
    //std::string json = "{\"timestamp\":1566446290225,\"status\":400,\"error\":\"Bad Request\",\"message\":\"Required String parameter 'domain' is not present\",\"path\":\"/live/api/inner/stream/callback\"}";
    SrsJsonAny* info = SrsJsonAny::loads((char*)res.c_str());
    if (!info) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid response %s. ret=%d", res.c_str(), ret);
        return ret;
    }
    SrsAutoFree(SrsJsonAny, info);
    
    // response error code in string.
    if (!info->is_object()) {
        if (res != SRS_HTTP_RESPONSE_OK) {
            ret = ERROR_HTTP_DATA_INVALID;
            srs_error("invalid response number %s. ret=%d", res.c_str(), ret);
            return ret;
        }
        return ret;
    }
    
    // response standard object, format in json: {"status": 0, "data": ""}
    SrsJsonObject* res_info = info->to_object();
        
    SrsJsonAny* status_code = NULL;
    if ((status_code = res_info->ensure_property_integer("status")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
    if ((status_code->to_integer()) != ERROR_SUCCESS) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("error response code=%d. ret=%d", status_code->to_integer(), ret);
        return ret;
    }
    SrsJsonAny* ip_info = NULL;
    if ((ip_info = res_info->ensure_property_string("ip")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
        
    SrsJsonAny* port_info = NULL;
    if ((port_info = res_info->ensure_property_string("port")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
        
    SrsJsonAny* recover_flag = NULL;
	if ((recover_flag = res_info->ensure_property_string("recover_flag")) == NULL) {
		req->recover_flag = false;
	} else {
		req->recover_flag = true;
	}
    ip = ip_info->to_str();
    port = port_info->to_str();
    return ret;
}

int SrsHttpHooks::on_scheduler_release_record_node(std::string url, SrsRequest* req) 
{
    int ret = ERROR_SUCCESS;
    int code;
    string res;
    std::stringstream ss;
    std::vector<std::string> ip_ports = _srs_config->get_listens();
    srs_assert((int)ip_ports.size() > 0);

    std::string local_ip;
    std::string local_port;
    for (int i = 0; i < (int)ip_ports.size(); i++) {
        srs_parse_endpoint(ip_ports[i], local_ip, local_port);
    }
    
	std::string cluster_id = _srs_config->get_cluster_clusterid();        
    std::vector<std::string> ips = srs_get_local_ipv4_ips();
    assert(_srs_config->get_stats_network() < (int)ips.size());
    local_ip = ips[_srs_config->get_stats_network()];

    ss << SRS_JOBJECT_START
        << SRS_JFIELD_STR("action", "ReleaseRecordNode") << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("domain", req->domain) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("appName", req->app) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("streamName", req->stream) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("cluster_id", cluster_id) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("ip", local_ip) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("port", local_port) 
        << SRS_JOBJECT_END;
    
    SrsHttpUri uri;
    if ((ret = uri.initialize(url)) != ERROR_SUCCESS) {
        srs_error("http: post failed. url=%s, ret=%d", url.c_str(), ret);
        return ret;
    }
    
    SrsHttpClient http;
    if ((ret = http.initialize(uri.get_host(), uri.get_port())) != ERROR_SUCCESS) {
        return ret;
    }
    
    ISrsHttpMessage* msg = NULL;
    if ((ret = http.post(uri.get_path(), ss.str(), &msg,"",true)) != ERROR_SUCCESS) {
        return ret;
    }
    SrsAutoFree(ISrsHttpMessage, msg);
    
    code = msg->status_code();
    if ((ret = msg->body_read_all(res)) != ERROR_SUCCESS) {
        return ret;
    }
    if (code != SRS_CONSTS_HTTP_OK) {
        ret = ERROR_HTTP_STATUS_INVALID;
        srs_error("invalid response status=%d. ret=%d", code, ret);
        return ret;
    }
    
    // should never be empty.
    if (res.empty()) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid empty response. ret=%d", ret);
        return ret;
    }
    srs_collection("http response: %s ",res.c_str());
    
    // parse string res to json.
    //std::string json = "{\"status\":1,\"code\":\"0001\",\"data\":{\"status\":0} }";
    //std::string json = "{\"timestamp\":1566446290225,\"status\":400,\"error\":\"Bad Request\",\"message\":\"Required String parameter 'domain' is not present\",\"path\":\"/live/api/inner/stream/callback\"}";
    SrsJsonAny* info = SrsJsonAny::loads((char*)res.c_str());
    if (!info) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid response %s. ret=%d", res.c_str(), ret);
        return ret;
    }
    SrsAutoFree(SrsJsonAny, info);
    
    // response error code in string.
    if (!info->is_object()) {
        if (res != SRS_HTTP_RESPONSE_OK) {
            ret = ERROR_HTTP_DATA_INVALID;
            srs_error("invalid response number %s. ret=%d", res.c_str(), ret);
            return ret;
        }
        return ret;
    }
    
    // response standard object, format in json: {"status": 0, "data": ""}
    SrsJsonObject* res_info = info->to_object();
        
    SrsJsonAny* status_code = NULL;
    if ((status_code = res_info->ensure_property_integer("status")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
    if ((status_code->to_integer()) != ERROR_SUCCESS) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("error response code=%d. ret=%d", status_code->to_integer(), ret);
        return ret;
    }
    return ret;
}

int SrsHttpHooks::on_scheduler_record_start(string url, SrsRequest* req)
{
    int ret = ERROR_SUCCESS;
    int code;
    string res;
    std::stringstream ss;
    std::vector<std::string> ip_ports = _srs_config->get_listens();
    srs_assert((int)ip_ports.size() > 0);

    std::string local_ip;
    std::string local_port;
    for (int i = 0; i < (int)ip_ports.size(); i++) {
        srs_parse_endpoint(ip_ports[i], local_ip, local_port);
    }

    std::string cluster_id = _srs_config->get_cluster_clusterid();        
    std::vector<std::string> ips = srs_get_local_ipv4_ips();
    assert(_srs_config->get_stats_network() < (int)ips.size());
    local_ip = ips[_srs_config->get_stats_network()];

    ss << SRS_JOBJECT_START
        << SRS_JFIELD_STR("action", "RecordStreamStart") << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("taskId", req->uuid) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("domain", req->domain) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("appName", req->app) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("streamName", req->stream) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("cluster_id", cluster_id) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("ip", local_ip) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("port", local_port) 
        << SRS_JOBJECT_END;

    SrsHttpUri uri;
    if ((ret = uri.initialize(url)) != ERROR_SUCCESS) {
        srs_error("http: post failed. url=%s, ret=%d", url.c_str(), ret);
        return ret;
    }

    SrsHttpClient http;
    if ((ret = http.initialize(uri.get_host(), uri.get_port())) != ERROR_SUCCESS) {
        return ret;
    }

    ISrsHttpMessage* msg = NULL;
    if ((ret = http.post(uri.get_path(), ss.str(), &msg,"",true)) != ERROR_SUCCESS) {
        return ret;
    }
    SrsAutoFree(ISrsHttpMessage, msg);

    code = msg->status_code();
    if ((ret = msg->body_read_all(res)) != ERROR_SUCCESS) {
        return ret;
    }
    if (code != SRS_CONSTS_HTTP_OK) {
        ret = ERROR_HTTP_STATUS_INVALID;
        srs_error("invalid response status=%d. ret=%d", code, ret);
        return ret;
    }

    // should never be empty.
    if (res.empty()) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid empty response. ret=%d", ret);
        return ret;
    }
    srs_collection("http response: %s ",res.c_str());

    // parse string res to json.
    //std::string json = "{\"status\":1,\"code\":\"0001\",\"data\":{\"status\":0} }";
    //std::string json = "{\"timestamp\":1566446290225,\"status\":400,\"error\":\"Bad Request\",\"message\":\"Required String parameter 'domain' is not present\",\"path\":\"/live/api/inner/stream/callback\"}";
    SrsJsonAny* info = SrsJsonAny::loads((char*)res.c_str());
    if (!info) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid response %s. ret=%d", res.c_str(), ret);
        return ret;
    }
    SrsAutoFree(SrsJsonAny, info);

    // response error code in string.
    if (!info->is_object()) {
        if (res != SRS_HTTP_RESPONSE_OK) {
            ret = ERROR_HTTP_DATA_INVALID;
            srs_error("invalid response number %s. ret=%d", res.c_str(), ret);
            return ret;
        }
        return ret;
    }

    // response standard object, format in json: {"status": 0, "data": ""}
    SrsJsonObject* res_info = info->to_object();

    SrsJsonAny* status_code = NULL;
    if ((status_code = res_info->ensure_property_integer("status")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
    if ((status_code->to_integer()) != ERROR_SUCCESS) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("error response code=%d. ret=%d", status_code->to_integer(), ret);
        return ret;
    }
    return ret;
}

int SrsHttpHooks::on_scheduler_record_update(string url, SrsRequest* req, string file)
{
    int ret = ERROR_SUCCESS;
    int code;
    string res;
    std::stringstream ss;
    /*
    std::vector<std::string> ip_ports = _srs_config->get_listens();
    srs_assert((int)ip_ports.size() > 0);

    std::string local_ip;
    std::string local_port;
    for (int i = 0; i < (int)ip_ports.size(); i++) {
        srs_parse_endpoint(ip_ports[i], local_ip, local_port);
    }

    std::string cluster_id = _srs_config->get_cluster_clusterid();        
    std::vector<std::string> ips = srs_get_local_ipv4_ips();
    assert(_srs_config->get_stats_network() < (int)ips.size());
    local_ip = ips[_srs_config->get_stats_network()];
    */
    ss << SRS_JOBJECT_START
        << SRS_JFIELD_STR("action", "RecordStreamUpdateS") << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("taskId", req->uuid) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("domain", req->domain) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("appName", req->app) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("streamName", req->stream) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("file", file) /* << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("cluster_id", cluster_id) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("ip", local_ip) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("port", local_port) */
        << SRS_JOBJECT_END;

    SrsHttpUri uri;
    if ((ret = uri.initialize(url)) != ERROR_SUCCESS) {
        srs_error("http: post failed. url=%s, ret=%d", url.c_str(), ret);
        return ret;
    }

    SrsHttpClient http;
    if ((ret = http.initialize(uri.get_host(), uri.get_port())) != ERROR_SUCCESS) {
        return ret;
    }

    ISrsHttpMessage* msg = NULL;
    if ((ret = http.post(uri.get_path(), ss.str(), &msg,"",true)) != ERROR_SUCCESS) {
        return ret;
    }
    SrsAutoFree(ISrsHttpMessage, msg);

    code = msg->status_code();
    if ((ret = msg->body_read_all(res)) != ERROR_SUCCESS) {
        return ret;
    }
    if (code != SRS_CONSTS_HTTP_OK) {
        ret = ERROR_HTTP_STATUS_INVALID;
        srs_error("invalid response status=%d. ret=%d", code, ret);
        return ret;
    }

    // should never be empty.
    if (res.empty()) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid empty response. ret=%d", ret);
        return ret;
    }
    srs_collection("http response: %s ",res.c_str());

    // parse string res to json.
    //std::string json = "{\"status\":1,\"code\":\"0001\",\"data\":{\"status\":0} }";
    //std::string json = "{\"timestamp\":1566446290225,\"status\":400,\"error\":\"Bad Request\",\"message\":\"Required String parameter 'domain' is not present\",\"path\":\"/live/api/inner/stream/callback\"}";
    SrsJsonAny* info = SrsJsonAny::loads((char*)res.c_str());
    if (!info) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid response %s. ret=%d", res.c_str(), ret);
        return ret;
    }
    SrsAutoFree(SrsJsonAny, info);

    // response error code in string.
    if (!info->is_object()) {
        if (res != SRS_HTTP_RESPONSE_OK) {
            ret = ERROR_HTTP_DATA_INVALID;
            srs_error("invalid response number %s. ret=%d", res.c_str(), ret);
            return ret;
        }
        return ret;
    }

    // response standard object, format in json: {"status": 0, "data": ""}
    SrsJsonObject* res_info = info->to_object();

    SrsJsonAny* status_code = NULL;
    if ((status_code = res_info->ensure_property_integer("status")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
    if ((status_code->to_integer()) != ERROR_SUCCESS) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("error response code=%d. ret=%d", status_code->to_integer(), ret);
        return ret;
    }
    return ret;
}


int SrsHttpHooks::on_scheduler_record_stop(string url, SrsRequest* req)
{
    int ret = ERROR_SUCCESS;
    int code;
    string res;
    std::stringstream ss;
    std::vector<std::string> ip_ports = _srs_config->get_listens();
    srs_assert((int)ip_ports.size() > 0);

    std::string local_ip;
    std::string local_port;
    for (int i = 0; i < (int)ip_ports.size(); i++) {
        srs_parse_endpoint(ip_ports[i], local_ip, local_port);
    }

    std::string cluster_id = _srs_config->get_cluster_clusterid();        
    std::vector<std::string> ips = srs_get_local_ipv4_ips();
    assert(_srs_config->get_stats_network() < (int)ips.size());
    local_ip = ips[_srs_config->get_stats_network()];

    ss << SRS_JOBJECT_START
        << SRS_JFIELD_STR("action", "RecordStreamStop") << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("taskId", req->uuid) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("domain", req->domain) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("vhost", req->vhost) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("appName", req->app) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("streamName", req->stream) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("cluster_id", cluster_id) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("ip", local_ip) << SRS_JFIELD_CONT
        << SRS_JFIELD_STR("port", local_port) 
        << SRS_JOBJECT_END;

    SrsHttpUri uri;
    if ((ret = uri.initialize(url)) != ERROR_SUCCESS) {
        srs_error("http: post failed. url=%s, ret=%d", url.c_str(), ret);
        return ret;
    }

    SrsHttpClient http;
    if ((ret = http.initialize(uri.get_host(), uri.get_port())) != ERROR_SUCCESS) {
        return ret;
    }

    ISrsHttpMessage* msg = NULL;
    if ((ret = http.post(uri.get_path(), ss.str(), &msg,"",true)) != ERROR_SUCCESS) {
        return ret;
    }
    SrsAutoFree(ISrsHttpMessage, msg);

    code = msg->status_code();
    if ((ret = msg->body_read_all(res)) != ERROR_SUCCESS) {
        return ret;
    }
    if (code != SRS_CONSTS_HTTP_OK) {
        ret = ERROR_HTTP_STATUS_INVALID;
        srs_error("invalid response status=%d. ret=%d", code, ret);
        return ret;
    }

    // should never be empty.
    if (res.empty()) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid empty response. ret=%d", ret);
        return ret;
    }
    srs_collection("http response: %s ",res.c_str());

    // parse string res to json.
    //std::string json = "{\"status\":1,\"code\":\"0001\",\"data\":{\"status\":0} }";
    //std::string json = "{\"timestamp\":1566446290225,\"status\":400,\"error\":\"Bad Request\",\"message\":\"Required String parameter 'domain' is not present\",\"path\":\"/live/api/inner/stream/callback\"}";
    SrsJsonAny* info = SrsJsonAny::loads((char*)res.c_str());
    if (!info) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid response %s. ret=%d", res.c_str(), ret);
        return ret;
    }
    SrsAutoFree(SrsJsonAny, info);

    // response error code in string.
    if (!info->is_object()) {
        if (res != SRS_HTTP_RESPONSE_OK) {
            ret = ERROR_HTTP_DATA_INVALID;
            srs_error("invalid response number %s. ret=%d", res.c_str(), ret);
            return ret;
        }
        return ret;
    }

    // response standard object, format in json: {"status": 0, "data": ""}
    SrsJsonObject* res_info = info->to_object();

    SrsJsonAny* status_code = NULL;
    if ((status_code = res_info->ensure_property_integer("status")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
    if ((status_code->to_integer()) != ERROR_SUCCESS) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("error response code=%d. ret=%d", status_code->to_integer(), ret);
        return ret;
    }
    return ret;
}

SrsHttpNotify::SrsHttpNotify()
{
}

SrsHttpNotify::~SrsHttpNotify()
{
}

int SrsHttpNotify::do_notify(SrsRequest* req, string msg)
{
    int ret = ERROR_SUCCESS;
    /*
    int client_id = _srs_context->get_id();
    
    int64_t current_time = srs_update_system_time_ms();
    std::stringstream time_ss;
    time_ss << current_time;
 

    std::string domain = srs_get_param_key(req->param,"domain");    
    std::stringstream ss;

    std::string http_header="";
    
    SrsConfDirective* conf_key = _srs_config->get_vhost_http_header_key(req->vhost);
    SrsConfDirective* conf_value = _srs_config->get_vhost_http_header_value(req->vhost);
    if(conf_key && conf_value) {
        std::stringstream str_header;
        str_header << conf_key->args[0]
                   << ": "
                   << conf_value->args[0];
        http_header = str_header.str();
    }

    ss  << "aType=" << 3 
        << "&domain=" << domain
        << "&vhost=" << req->vhost
      	<< "&appName=" << req->app
	    << "&streamName=" << req->stream
	    << "&time=" << time_ss.str();
 

 
    std::string data = ss.str();
    std::string res;
    int status_code;
    if ((ret = do_post(url, data, status_code, res,http_header)) != ERROR_SUCCESS) {
        srs_collection("http post on_play uri failed. "
            "client_id=%d, url=%s, request=%s, response=%s, code=%d, ret=%d",
            client_id, url.c_str(), data.c_str(), res.c_str(), status_code, ret);

        srs_error("http post on_play uri failed. "
            "client_id=%d, url=%s, request=%s, response=%s, code=%d, ret=%d",
            client_id, url.c_str(), data.c_str(), res.c_str(), status_code, ret);
        return ret;
    }
    
    srs_collection("http hook on_play success. "
        "client_id=%d, url=%s, request=%s, response=%s, ret=%d",
        client_id, url.c_str(), data.c_str(), res.c_str(), ret);


    srs_trace("http hook on_play success. "
        "client_id=%d, url=%s, request=%s, response=%s, ret=%d",
        client_id, url.c_str(), data.c_str(), res.c_str(), ret);
    */
    return ret;
}

int SrsHttpNotify::do_post(std::string url, std::string req, int& code, string& res, const std::string http_header, bool is_json)
{
    int ret = ERROR_SUCCESS;
    /*
	int code;
	std::string res;
	SrsHttpUri uri;
    if ((ret = uri.initialize(url)) != ERROR_SUCCESS) {
        srs_error("http: post failed. url=%s, ret=%d", url.c_str(), ret);
        return ret;
    }
    
    SrsHttpClient http;
    if ((ret = http.initialize(uri.get_host(), uri.get_port())) != ERROR_SUCCESS) {
        return ret;
    }
    
    ISrsHttpMessage* msg = NULL;
    if ((ret = http.post(uri.get_path(), req, &msg, "", true)) != ERROR_SUCCESS) {
        return ret;
    }
    SrsAutoFree(ISrsHttpMessage, msg);
    
    code = msg->status_code();
    if ((ret = msg->body_read_all(res)) != ERROR_SUCCESS) {
        return ret;
    }
    
    // ensure the http status is ok.
    // https://github.com/ossrs/srs/issues/158
    if (code != SRS_CONSTS_HTTP_OK) {
        ret = ERROR_HTTP_STATUS_INVALID;
        srs_error("invalid response status=%d. ret=%d", code, ret);
        return ret;
    }
    
    // should never be empty.
    if (res.empty()) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid empty response. ret=%d", ret);
        return ret;
    }

    srs_collection("http response: %s ",res.c_str());
    // parse string res to json.
    //std::string json = "{\"status\":1,\"code\":\"0001\",\"data\":{\"status\":0} }";
    //std::string json = "{\"timestamp\":1566446290225,\"status\":400,\"error\":\"Bad Request\",\"message\":\"Required String parameter 'domain' is not present\",\"path\":\"/live/api/inner/stream/callback\"}";
    SrsJsonAny* info = SrsJsonAny::loads((char*)res.c_str());
    if (!info) {
        ret = ERROR_HTTP_DATA_INVALID;
        srs_error("invalid response %s. ret=%d", res.c_str(), ret);
        return ret;
    }
    SrsAutoFree(SrsJsonAny, info);
    
    
    // response error code in string.
    if (!info->is_object()) {
        if (res != SRS_HTTP_RESPONSE_OK) {
            ret = ERROR_HTTP_DATA_INVALID;
            srs_error("invalid response number %s. ret=%d", res.c_str(), ret);
            return ret;
        }
        return ret;
    }
    
    // response standard object, format in json: {"status": 0, "data": ""}
    SrsJsonObject* res_info = info->to_object();
    SrsJsonAny* res_code = NULL;
    if ((res_code = res_info->get_property("data")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
    
    SrsJsonObject* data  = NULL;
    if (!res_code->is_object() || (data = res_code->to_object()) == NULL ) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    } 
    SrsJsonAny* status_code = NULL;
    if ((status_code = data->ensure_property_integer("status")) == NULL) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("invalid response without code, ret=%d", ret);
        return ret;
    }
    if ((status_code->to_integer()) != ERROR_SUCCESS) {
        ret = ERROR_RESPONSE_CODE;
        srs_error("error response code=%d. ret=%d", status_code->to_integer(), ret);
        return ret;
    }
    */
    return ret;
}

#endif
