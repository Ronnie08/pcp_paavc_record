#!/usr/bin/env python
# -*- coding: utf-8 -*-

from xml.parsers.expat import ParserCreate


class CommonXmlResponseHandler(object):

    def __init__(self, root_ele_names, ele_names):
        self._root_ele_names = root_ele_names
        self._ele_names = ele_names

    def handle_success(self, response):
        handler = CommonSaxHandler(self._root_ele_names, self._ele_names)
        parser = ParserCreate()
        parser.StartElementHandler = handler.start_element
        parser.EndElementHandler = handler.end_element
        parser.CharacterDataHandler = handler.char_element
        parser.Parse(response.text)
        return CommonXmlParseResult(handler.root_ele_name, handler.elements)


class CommonXmlParseResult(object):

    def __init__(self, root_ele_name, ele_names):
        self._root_ele_name = root_ele_name
        self._ele_names = ele_names

    @property
    def root_ele_name(self):
        return self._root_ele_name

    @property
    def ele_names(self):
        return self._ele_names

    def __str__(self):
        return 'CommonXmlParseResult (root_ele_name: %s, ele_names: %s)' % (self._root_ele_name, self._ele_names)

    __repr__ = __str__


class CommonSaxHandler(object):

    def __init__(self, root_ele_names, ele_names):
        self._root_ele_names = root_ele_names
        self._ele_names = ele_names
        self._started_element = None
        self._ele_text = None
        self._first_ele_encountered = None
        self._root_ele_name = None
        self._elements = {}

    @property
    def root_ele_name(self):
        return self._root_ele_name

    @property
    def elements(self):
        return self._elements

    def start_element(self, name, attrs):
        self._started_element = name
        if name in self._root_ele_names:
            if not self._first_ele_encountered:
                self._root_ele_name = name
                self._first_ele_encountered = True
        else:
            self._ele_text = ''

    def char_element(self, text):
        self._ele_text += text

    def end_element(self, name):
        if name in self._ele_names and name == self._started_element:
            self._elements[name] = self._ele_text
        self._ele_text = ''
