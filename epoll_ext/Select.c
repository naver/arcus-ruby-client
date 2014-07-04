/*
 * arcus-ruby-client - Arcus ruby client drvier
 * Copyright 2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 

#include "ruby.h"
#include "ruby/intern.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <sys/ioctl.h>


static VALUE module_select;
static VALUE class_select_epoll;

#define EPOLL_SIZE 1024

typedef struct {
	int efd;
	struct epoll_event *events;
} st_epoll;

static void method_free(void *p)
{
	st_epoll *ep = (st_epoll*)p;

	close(ep->efd);
	free(ep->events);
	free(ep);
}

static VALUE method_alloc(VALUE klass)
{
	st_epoll *ep;
	VALUE obj;
	
	ep = (st_epoll *)malloc(sizeof(st_epoll));

	ep->efd = epoll_create(EPOLL_SIZE);
	if (ep->efd < 0) {
		printf("epoll_create fail");
	}

	ep->events = (struct epoll_event *)malloc(sizeof(struct epoll_event) * EPOLL_SIZE);
	if (ep->events == NULL) {
		printf("epoll_create fail");
	}

	obj = Data_Wrap_Struct(klass, 0, method_free, ep);
	return obj;
}

VALUE method_initialize(VALUE self)
{
	st_epoll *ep;
	Data_Get_Struct(self, st_epoll, ep);
	
	// do something for init for ep

	return self;
}

VALUE method_register(VALUE self, VALUE fileno, VALUE mask)
{
	int ret;
	struct epoll_event ev;

	st_epoll *ep;
	Data_Get_Struct(self, st_epoll, ep);
	
	ev.events = NUM2INT(mask);
	ev.data.fd = NUM2INT(fileno);
	
	ret = epoll_ctl(ep->efd, EPOLL_CTL_ADD, NUM2INT(fileno), &ev);
	return INT2NUM(ret);
}

VALUE method_unregister(VALUE self, VALUE fileno)
{
	int ret;
	st_epoll *ep;
	Data_Get_Struct(self, st_epoll, ep);

	ret = epoll_ctl(ep->efd, EPOLL_CTL_DEL, NUM2INT(fileno), ep->events);
	return INT2NUM(ret);
}

typedef struct 
{
	VALUE self;
	double timeout;
	VALUE fds;
	VALUE events;
} self_timeout;


VALUE method_poll_sub(void* param)
{
	self_timeout *st = (self_timeout*)param;
	int i, ret;
	st_epoll *ep;
	VALUE fds = st->fds;
	VALUE events = st->events;
	
	Data_Get_Struct(st->self, st_epoll, ep);

	ret = epoll_wait(ep->efd, ep->events, EPOLL_SIZE, (int)st->timeout);

	for (i=0; i<ret; i++) {
		rb_ary_push(fds, INT2NUM(ep->events[i].data.fd));
		rb_ary_push(events, INT2NUM(ep->events[i].events));
	}

	return INT2NUM(0);
}

VALUE method_poll(VALUE self, VALUE timeout, VALUE fds, VALUE events)
{
	self_timeout st;

	st.self = self;
	st.timeout = NUM2DBL(timeout) * 1000; // 1000 for sec to msec
	st.fds = fds;
	st.events = events;
	if (st.timeout < 0) st.timeout = -1;

	return rb_thread_blocking_region((rb_blocking_function_t*)method_poll_sub, (void*)&st, (rb_unblock_function_t*)-1, (void*)0);
}


void Init_Select()
{
	module_select = rb_define_module("Select");

	// class Epoll
	class_select_epoll = rb_define_class_under(module_select, "Epoll", rb_cObject);
	rb_define_alloc_func(class_select_epoll, method_alloc);

	rb_define_method(class_select_epoll, "initialize", method_initialize, 0);
	rb_define_method(class_select_epoll, "register", method_register, 2);
	rb_define_method(class_select_epoll, "unregister", method_unregister, 1);
	rb_define_method(class_select_epoll, "poll", method_poll, 3);

	// constants
	rb_define_const(module_select, "EPOLLIN", INT2NUM(EPOLLIN));
	rb_define_const(module_select, "EPOLLOUT", INT2NUM(EPOLLOUT));
	rb_define_const(module_select, "EPOLLERR", INT2NUM(EPOLLERR));
	rb_define_const(module_select, "EPOLLHUP", INT2NUM(EPOLLHUP));
	rb_define_const(module_select, "EPOLLPRI", INT2NUM(EPOLLPRI));
	rb_define_const(module_select, "EPOLLET", INT2NUM(EPOLLET));
}


