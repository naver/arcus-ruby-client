#
# arcus-ruby-client - Arcus ruby client drvier
# Copyright 2014 NAVER Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
 

require 'socket'
require 'pp'
#require 'Select'
require 'set'

require './arcus'

def len(a)
	return a.length
end


def isdigit(a)
	a.match(/^[0-9]+$/) == nil ? false : true
end

class Connection
	attr_reader :socket
	attr_writer :socket
	
	def initialize(host)
		ip, port = host.split(":")
		@ip = ip
		@port = port.to_i
		@address = [ @ip, @port ]

		@socket = nil
		@buffer = ""

		self.connect()
	end

	def connect()
		if @socket
			disconnect()
		end

		@socket = TCPSocket.new(@ip, @port)
		# TODO: exception handling

		@buffer = ""
		return @socket
	end

	def disconnect()
		if @socket
			@socket.close()
			@socket = nil
		end
	end

	def disconnected()
		return @socket == nil
	end

	def send_request(request)
		arcuslog(self, "send_request: '%s'" % (request + "\r\n"))
		@socket.send(request + "\r\n", 0)
	end

	def hasline()
		index = @buffer.index("\r\n")
		return index != nil
	end

	def readline()
		buf = @buffer

		while true
			index = buf.index("\r\n")
			if index != nil
				break
			end

			data = @socket.recv(4096)
			arcuslog(self, "sock recv: (%d) %s" % [len(data), data])

			if data == nil
				self.disconnect()
				raise ArcusNodeConnectionException("connection lost")
			end

			buf += data
		end

		@buffer = buf[index+2..-1]

		return buf[0..index-1]
	end

	def recv(rlen)
		buf = @buffer
		while len(buf) < rlen
			foo = @socket.recv(max(rlen - len(buf), 4096))
			arcuslog(self, "sock recv: (%d) %s " % [len(foo), foo])

			buf += foo
			if foo == nil
				raise ArcusNodeSocketException("Read %d bytes, expecting %d, read returned 0 length bytes" % [len(buf), rlen])
			end
		end

		@buffer = buf[rlen..-1]
		arcuslog(self, "recv: ", buf[0..rlen-1])
		return buf[0..rlen-1]
	end
end


class ArcusMCNode
	attr_reader :in_use, :addr, :name, :transcoder, :ops, :lock
	attr_writer :in_use, :addr, :name, :transcoder, :ops, :lock

	@@worker = nil
	@@shutdown = false

	def self.worker(v=nil)
		if v != nil
			@@worker = v
		end

		return @@worker
	end

	def self.shutdown(v=nil)
		if v != nil
			@@shutdown = v
		end

		return @@shutdown
	end

	def initialize(addr, name, transcoder)
		#mandatory files
		@addr = addr
		@name = name
		@in_use = false
		@transcoder = transcoder

		@handle = Connection.new(addr)
		@ops = []
		@lock = Mutex.new # for ordering worker.q and ops
	end

	def to_s
		return "%s-%s" % [@addr, @name]
	end

	def get_fileno()
		return @handle.socket.fileno()
		#return @handle.socket
	end

	def disconnect()
		# disconnect socket
		@handle.disconnect()

		# clear existing operation
		for op in @ops
			op.set_invalid()
		end

		@ops = []
	end
		
	def disconnect_all() # shutdown
		@@shutdown = true
		self.disconnect()

		@@worker.q.push(nil)
	end
		
	def process_request(request)
		if @handle.disconnected()
			ret = @handle.connect()
			if ret != nil
				# re-register if node connection is available
				@worker.register_node(self)
			end
		end

		@handle.send_request(request)
	end



	##########################################################################################
	### commands
	##########################################################################################
	def get(key)
		return self._get("get", key)
	end

	def gets(key)
		return self._get("gets", key)
	end

	def set(key, val, exptime=0)
		return self._set("set", key, val, exptime)
	end

	def cas(key, val, cas_id, exptime=0)
		return self._cas(key, "cas", val, cas_id, exptime)
	end

	def incr(key, value=1)
		return self._incr_decr("incr", key, value)
	end

	def decr(key, value=1)
		return self._incr_decr("decr", key, value)
	end

	def add(key, val, exptime=0)
		return self._set("add", key, val, exptime)
	end

	def append(key, val, exptime=0)
		return self._set("append", key, val, exptime)
	end

	def prepend(key, val, exptime=0)
		return self._set("prepend", key, val, exptime)
	end

	def replace(key, val, exptime=0)
		return self._set("replace", key, val, exptime)
	end

	def delete(key, exptime=0)
		if exptime != nil and exptime != 0
			full_cmd = "delete %s %d" % [key, exptime]
		else
			full_cmd = "delete %s" % key
		end

		return self.add_op("delete", full_cmd, method(:_recv_delete))
	end

	def flush_all()
		full_cmd = "flush_all"
		return self.add_op("flush_all", full_cmd, method(:_recv_ok))
	end

	def get_stats(stat_args = nil)
		if stat_args == nil
			full_cmd = "stats"
		else
			full_cmd = "stats " + stat_args
		end

		op = self.add_op("stats", full_cmd, method(:_recv_stat))
	end

	def lop_create(key, flags, exptime=0, noreply=false, attr=nil)
		return self._coll_create("lop create", key, flags, exptime, noreply, attr)
	end

	def lop_insert(key, index, value, noreply=false, pipe=false, attr=nil)
		return self._coll_set("lop insert", key, index, value, noreply, pipe, attr)
	end

	def lop_delete(key, range, drop=false, noreply=false, pipe=false)
		option = ""
		if drop == true
			option += "drop"
		end

		if noreply == true
			option += " noreply"
		end

		if pipe == true
			assert noreply == false
			option += " pipe"
		end

		if range.is_a?(Array)
			full_cmd = "log delete %s %d..%d %s " % [key, range[0], range[1], option]
			return self.add_op('lop delete', full_cmd, method(:_recv_delete), (noreply or pipe))

		else
			full_cmd = "log delete %s %d %s " % [key, range, option]
			return self.add_op('lop delete', full_cmd, method(:_recv_delete), (noreply or pipe))
		end
	end


	def lop_get(key, range, delete=false, drop=false)
		return self._coll_get("lop get", key, range, method(:_recv_lop_get), delete, drop)
	end

	def sop_create(key, flags, exptime=0, noreply=false, attr=nil)
		return self._coll_create("sop create", key, flags, exptime, noreply, attr)
	end

	def sop_insert(key, value, noreply=false, pipe=false, attr=nil)
		return self._coll_set("sop insert", key, nil, value, noreply, pipe, attr)
	end

	def sop_get(key, count=0, delete=false, drop=false)
		return self._coll_get("sop get", key, count, method(:_recv_sop_get), delete, drop)
	end

	def sop_delete(key, val, drop=false, noreply=false, pipe=false)
		flags, len, value = self.transcoder.encode(val)

		option = "%d" % len
		if drop == true
			option += "drop"
		end

		if noreply == true
			option += " noreply"
		end

		if pipe == true
			assert noreply == false
			option += " pipe"
		end

		option += "\r\n"
		option = option + value

		full_cmd = "sop delete %s %s" % [key, option]
		return self.add_op('sop delete', full_cmd, method(:_recv_delete), (noreply or pipe))
	end

	def sop_exist(key, val, pipe=false)
		flags, len, value = self.transcoder.encode(val)

		option = "%d" % len
		if pipe == true
			assert noreply == false
			option += " pipe"
		end

		option += "\r\n"
		option = option + value

		full_cmd = "sop exist %s %s" % [key, option]
		return self.add_op('sop exist', full_cmd, method(:_recv_exist), pipe)
	end

	def bop_create(key, flags, exptime=0, noreply=false, attr=nil)
		return self._coll_create("bop create", key, flags, exptime, noreply, attr)
	end

	def bop_insert(key, bkey, value, eflag=nil, noreply=false, pipe=false, attr=nil)
		return self._coll_set("bop insert", key, nil, value, noreply, pipe, attr, bkey, eflag)
	end

	def bop_upsert(key, bkey, value, eflag=nil, noreply=false, pipe=false, attr=nil)
		return self._coll_set("bop upsert", key, nil, value, noreply, pipe, attr, bkey, eflag)
	end

	def bop_update(key, bkey, value, eflag=nil, noreply=false, pipe=false, attr=nil)
		return self._coll_set("bop update", key, nil, value, noreply, pipe, attr, bkey, eflag)
	end

	def bop_delete(key, range, filter=nil, count=nil, drop=false, noreply=false, pipe=false)
		option = ""

		if filter != nil
			option += filter.get_expr() + " "
		end

		if count != nil
			option += "%d " % count
		end

		if drop == true
			option += "drop"
		end

		if noreply == true
			option += " noreply"
		end

		if pipe == true
			assert noreply == false
			option += " pipe"
		end

		if range.is_a?(Array)
			if range[0].is_a?(String)
				if range[0][0..1] != "0x" or range[1][0..1] != "0x"
					raise CollectionHexFormat()
				end

				full_cmd = "bop delete %s..%s %s " % [key, range[0], range[1], option]
				return self.add_op('bop delete', full_cmd, method(:_recv_delete), (noreply or pipe))
			else
				full_cmd = "bop delete %d..%d %s " % [key, range[0], range[1], option]
				return self.add_op('bop delete', full_cmd, method(:_recv_delete), (noreply or pipe))
			end
		else
			if range.is_a?(String)
				if range[0..1] != "0x"
					raise CollectionHexFormat()
				end

				full_cmd = "bop delete %s %s " % [key, range, option]
				return self.add_op('bop delete', full_cmd, method(:_recv_delete), (noreply or pipe))
			else
				full_cmd = "bop delete %d %s " % [key, range, option]
				return self.add_op('bop delete', full_cmd, method(:_recv_delete), (noreply or pipe))
			end
		end
	end

	def bop_get(key, range, filter=nil, delete=false, drop=false)
		return self._coll_get("bop get", key, range, method(:_recv_bop_get), delete, drop, filter=filter)
	end

	def bop_mget(key_list, range, filter=nil, offset=nil, count=50)
		return self._coll_mget("bop mget", key_list, range, filter, offset, count)
	end

	def bop_smget(key_list, range, filter=nil, offset=nil, count=2000)
		return self._coll_mget("bop smget", key_list, range, filter, offset, count)
	end

	def bop_count(key, range, filter)
		return self._coll_get("bop count", key, range, method(:_recv_bop_get), filter=filter)
	end

	def bop_incr(key, bkey, value, noreply=false, pipe=false)
		return self._bop_incrdecr("bop incr", key, bkey, value, noreply, pipe)
	end
		
	def bop_decr(key, bkey, value, noreply=false, pipe=false)
		return self._bop_incrdecr("bop decr", key, bkey, value, noreply, pipe)
	end


	##########################################################################################
	### Queue senders
	##########################################################################################
	def add_op(cmd, full_cmd, callback, noreply = false)
		op = ArcusOperation.new(self, full_cmd, callback)
		arcuslog(self, "add operation %s(%s) to %s" % [full_cmd, callback, self])

			
		if noreply # or pipe
			# don"t need to receive response, set_result now
			ArcusMCNode.worker.q.push(op)
			op.set_result(true)
		else
			@lock.lock()
			ArcusMCNode.worker.q.push(op)
			@ops.push(op)
			@lock.unlock()
		end

		return op
	end

	def _get(cmd, key)
		full_cmd = "%s %s" % [cmd, key]
		if cmd == "gets"
			callback = method(:_recv_cas_value)
		else
			callback = method(:_recv_value)
		end

		op = self.add_op(cmd, full_cmd, callback)
		return op
	end

	def _set(cmd, key, val, exptime=0)

		flags, len, value = @transcoder.encode(val)
		if flags == nil
			return(0)
		end

		full_cmd = "%s %s %d %d %d\r\n" % [cmd, key, flags, exptime, len]
		full_cmd = full_cmd + value

		op = self.add_op(cmd, full_cmd, method(:_recv_set))
		return op 
	end

	def _cas(cmd, key, val, cas_id, exptime=0)
		flags, len, value = @transcoder.encode(val)
		if flags == nil
			return(0)
		end

		full_cmd = "%s %s %d %d %d %d\r\n" % [cmd, key, flags, exptime, len, cas_id]
		full_cmd = full_cmd + value

		op = self.add_op(cmd, full_cmd, method(:_recv_set))
		return op 
	end

	def _incr_decr(cmd, key, value)
		full_cmd = "%s %s %d" % [cmd, key, value]

		op = self.add_op(cmd, full_cmd, method(:_recv_set))
		return op
	end

	def _coll_create(cmd, key, flags, exptime=0, noreply=false, attr=nil)
		if attr == nil
			attr = {}
		end

		# default value
		if !attr.has_key?("maxcount")
			attr["maxcount"] = 4000
		end

		if !attr.has_key?("ovflaction")
			attr["ovflaction"] = "tail_trim"
		end

		if !attr.has_key?("readable")
			attr["readable"] = true
		end

		option = "%d %d %d" % [flags, exptime, attr["maxcount"]]
		if attr["ovflaction"] != "tail_trim"
			option += " " + attr["ovflaction"]
		end

		if attr["readable"] == false
			option += " unreadable"
		end

		if noreply == true
			option += " noreply"
		end

		full_cmd = "%s %s %s" % [cmd, key, option]
		return self.add_op(cmd, full_cmd, method(:_recv_coll_create), noreply)
	end

	def _bop_incrdecr(cmd, key, bkey, val, noreply=false, pipe=false)
		if val.is_a?(Fixnum)
			value = "%d" % val
		else
			value = val
		end

		if bkey.is_a?(Fixnum)
			bkey_str = "%d" % bkey
		else
			if bkey[0..1] != "0x"
				raise CollectionHexFormat()
			end
			bkey_str = "%s" % bkey
		end

		option = "%s %s" % [bkey_str, value]
	
		if noreply == true
			option += " noreply"
		end

		if pipe == true
			assert noreply == false
			option += " pipe"
		end

		full_cmd = "%s %s %s" % [cmd, key, option]
		return self.add_op(cmd, full_cmd, method(:_recv_set), (noreply or pipe))
	end

	def _coll_set(cmd, key, index, val, noreply=false, pipe=false, attr=nil, bkey=nil, eflag=nil)
		flags, len, value = self.transcoder.encode(val)

		if bkey != nil	# bop
			assert index == nil

			if bkey.is_a?(Fixnum)
				bkey_str = "%d" % bkey
			else
				if bkey[0..1] != "0x"
					raise CollectionHexFormat()
				end

				bkey_str = "%s" % bkey
			end

			if eflag != nil
				if eflag[0..1] != "0x"
					raise CollectionHexFormat()
				end

				option = "%s %s %d" % [bkey_str, eflag, len]
			else
				option = "%s %d" % [bkey_str, len]
			end
		elsif index != nil	# lop
			option = "%d %d" % [index, len]
		else			# sop
			option = "%d" % len
		end
	
		if attr != nil
			# default mandatory value
			if !attr.has_key?("flags")
				attr["flags"] = 0
			end

			if !attr.has_key?("exptime")
				attr["exptime"] = 0
			end

			if !attr.has_key?("maxcount")
				attr["maxcount"] = 4000
			end

			option += " create %d %d %d" % [attr["flags"], attr["exptime"], attr["maxcount"]]
			if !attr.has_key?("ovflaction")
				option += " " + attr["ovflaction"]
			end

			if attr.has_key?("readable") and attr["readable"] == false
				option += " unreadable"
			end
		end

		if noreply == true
			option += " noreply"
		end

		if pipe == true
			assert noreply == false
			option += " pipe"
		end

		option += "\r\n"
		option = option + value

		full_cmd = "%s %s %s" % [cmd, key, option]
		return self.add_op(cmd, full_cmd, method(:_recv_coll_set), (noreply or pipe))
	end

	def _coll_get(cmd, key, range, callback, delete=nil, drop=nil, filter=nil)
		option = ""
		type = cmd[0..2]

		if filter != nil
			option += filter.get_expr() + " "
		end

		if delete == true
			option += "delete"
		end

		if drop == true
			assert delete == false
			option += "drop"
		end

		if range.is_a?(Array)
			if type == "bop" and range[0].is_a?(String)
				if range[0][0..1] != "0x" or range[1][0..1] != "0x"
					raise CollectionHexFormat()
				end

				full_cmd = "%s %s %s..%s %s" % [cmd, key, range[0], range[1], option]
				return self.add_op(cmd, full_cmd, callback)
			else
				full_cmd = "%s %s %d..%d %s" % [cmd, key, range[0], range[1], option]
				return self.add_op(cmd, full_cmd, callback)
			end
		else
			if type == "bop" and range.is_a?(String)
				if range[0..1] != "0x"
					raise CollectionHexFormat()
				end

				full_cmd = "%s %s %s %s" % [cmd, key, range, option]
				return self.add_op(cmd, full_cmd, callback)
			else
				full_cmd = "%s %s %d %s" % [cmd, key, range, option]
				return self.add_op(cmd, full_cmd, callback)
			end
		end
	end


	def _coll_mget(org_cmd, key_list, range, filter, offset, count)

		comma_sep_keys = ""
		for key in key_list
			if comma_sep_keys != ""
				comma_sep_keys += ","
			end
			comma_sep_keys += key
		end

		cmd = "%s %d %d " % [org_cmd, len(comma_sep_keys), len(key_list)]

		if range.is_a?(Array)
			if range[0].is_a?(String)
				if range[0][0..1] != "0x" or range[1][0..1] != "0x"
					raise CollectionHexFormat()
				end

				cmd += "%s..%s" % range
			else
				cmd += "%d..%d" % range
			end
		else
			if range.is_a?(String)
				if range[0..1] != "0x"
					raise CollectionHexFormat()
				end

				cmd += "%s" % range
			else
				cmd += "%d" % range
			end
		end

		if filter != nil
			cmd += " " + filter.get_expr()
		end

		if offset != nil
			cmd += " %d %d" % [offset, count]
		else
			cmd += " %d" % count
		end

		cmd += "\r\n%s" % comma_sep_keys

		if org_cmd == "bop mget"
			op = self.add_op(org_cmd, cmd, method(:_recv_mget))
		else
			op = self.add_op(org_cmd, cmd, method(:_recv_smget))
		end

		return op 
	end

	

	##########################################################################################
	### recievers
	##########################################################################################
	def do_op()
		@lock.lock()

		if @ops.length <= 0
			op.set_invalid() # something wrong
			@lock.unlock()
		end

		op = @ops.shift()
		@lock.unlock()

		ret = op.callback.call()
		op.set_result(ret)

		while @handle.hasline() # remaining jobs
			@lock.lock()
			op = @ops.shift()
			@lock.unlock()

			ret = op.callback.call();
			op.set_result(ret)
		end
	end

	def _recv_ok()
		line = @handle.readline()
		if line == "OK"
			return true
		end

		return false
	end

	def _recv_stat()
		data = {}
		while true
			line = handle.readline()
			if line[3..-1] == "END" or line is nil
				break
			end

			dummy, k, v = line.split(" ", 3)
			data[k] = v

			return data
		end
	end

	def _recv_set()
		line = @handle.readline()
		if line[0..7] == "RESPONSE"
			dummy, count = line.split()

			ret = []
			for i in 0..count.to_i-1
				line = @handle.readline()
				ret.push(line)
			end

			line = @handle.readline() # "END"
		
			return ret
		end
				
			
		if line == "STORED"
			return true
		end

		if line == "NOT_FOUND"
			return false
		end

		if line == "TYPE_MISMATCH"
			raise CollectionType()
		end

		if line == "OVERFLOWED"
			raise CollectionOverflow() 
		end

		if line == "OUT_OF_RANGE"
			raise CollectionIndex()
		end

		if isdigit(line)	# incr, decr, bop incr, bop decr
			return line.to_i
		end

		return false
	end

	def _recv_delete()
		line = @handle.readline()
		if line[0..7] == "RESPONSE"
			dummy, count = line.split()

			ret = []
			for i in 0..count.to_i-1
				line = @handle.readline()
				ret.push(line)
			end

			line = @handle.readline() # "END"
		
			return ret
		end
				
		if line == "DELETED"
			return true
		end

		if line == "NOT_FOUND"
			return true # true ?? (or exception)
		end

		if line == "TYPE_MISMATCH"
			raise CollectionType()
		end

		if line == "OVERFLOWED"
			raise CollectionOverflow() 
		end

		if line == "OUT_OF_RANGE" or line == "NOT_FOUND_ELEMENT"
			raise CollectionIndex()
		end

		return false
	end

	def _recv_cas_value() 
		line = @handle.readline()
		if line and line[0..4] != "VALUE"
			return nil
		end

		resp, rkey, flags, len, cas_id = line.split()
		flags = flags.to_i
		rlen = len.to_i
		val = self._decode_value(flags, rlen)
		return [val, cas_id]
	end

	def _recv_value()
		line = @handle.readline()
		if line and line[0..4] != "VALUE"
			return nil
		end

		resp, rkey, flags, len = line.split()
		flags = flags.to_i
		rlen = len.to_i
		return self._decode_value(flags, rlen)
	end

	def _recv_coll_create()
		line = @handle.readline()
		if line == "CREATED"
			return true
		end

		if line == "EXISTS"
			raise CollectionExist()
		end

		return false
	end

	def _recv_coll_set()
		line = @handle.readline()
		if line[0..7] == "RESPONSE"
			dummy, count = line.split()

			ret = []
			for i in 0..count.to_i-1
				line = @handle.readline()
				ret.push(line)
			end

			line = @handle.readline() # "END"
		
			return ret
		end
				
		if line == "STORED"
			return true
		end

		if line == "NOT_FOUND"
			return false
		end

		if line == "TYPE_MISMATCH"
			raise CollectionType()
		end

		if line == "OVERFLOWED"
			raise CollectionOverflow() 
		end

		if line == "OUT_OF_RANGE"
			raise CollectionIndex()
		end

		return false
	end

	def _recv_lop_get()
		ret, value = self._decode_collection("lop")
		if ret == "NOT_FOUND"
			return nil
		end

		if ret == "TYPE_MISMATCH"
			raise CollectionType()
		end


		if ret == "UNREADABLE"
			raise CollectionUnreadable() 
		end

		if ret == "OUT_OF_RANGE" or ret == "NOT_FOUND_ELEMENT"
			value = []
		end

		return value
	end

	def _recv_sop_get()
		ret, value = self._decode_collection("sop")
		if ret == "NOT_FOUND"
			return nil
		end

		if ret == "TYPE_MISMATCH"
			raise CollectionType()
		end

		if ret == "UNREADABLE"
			raise CollectionUnreadable() 
		end

		if ret == "OUT_OF_RANGE" or ret == "NOT_FOUND_ELEMENT"
			value = Set.new
		end

		return value
	end

	def _recv_exist()
		line = @handle.readline()
		return line == "EXIST"
	end

	def _recv_bop_get()
		ret, value = self._decode_collection("bop")
		if ret == "NOT_FOUND"
			return nil
		end

		if ret == "TYPE_MISMATCH"
			raise CollectionType()
		end

		if ret == "UNREADABLE"
			raise CollectionUnreadable() 
		end

		if ret == "OUT_OF_RANGE" or ret == "NOT_FOUND_ELEMENT"
			value = {}
		end

		return value
	end

	def _recv_mget()
		ret, value, miss = self._decode_bop_mget()
		if ret == "NOT_FOUND"
			return nil
		end

		if ret == "TYPE_MISMATCH"
			raise CollectionType()
		end

		if ret == "UNREADABLE"
			raise CollectionUnreadable() 
		end

		if ret == "OUT_OF_RANGE" or ret == "NOT_FOUND_ELEMENT"
			raise CollectionIndex()
		end

		return [value, miss]
	end

	def _recv_smget()
		ret, value, miss = self._decode_bop_smget()
		if ret == "NOT_FOUND"
			return nil
		end

		if ret == "TYPE_MISMATCH"
			raise CollectionType()
		end

		if ret == "UNREADABLE"
			raise CollectionUnreadable() 
		end

		if ret == "OUT_OF_RANGE" or ret == "NOT_FOUND_ELEMENT"
			raise CollectionIndex()
		end

		return [value, miss]
	end



	##########################################################################################
	### decoders
	##########################################################################################
	def _decode_value(flags, rlen)
		rlen += 2 # include \r\n
		buf = @handle.recv(rlen)
		if len(buf) != rlen
			raise ArcusNodeSocketException("received %d bytes when expecting %d" % [len(buf), rlen])
		end

		if len(buf) == rlen
			buf = buf[0..-3]  # strip \r\n
		end

		val = @transcoder.decode(flags, buf)

		line = @handle.readline()
		if line != "END"
			raise ArcusProtocolException("invalid response expect END but recv: %s" % line)
		end

		return val
	end

	def _decode_collection(type)
		if type == "bop"
			values = {}
		elsif type == "sop"
			values = Set.new
		else # lop
			values = []
		end

		while true
			line = @handle.readline()
			if line[0..4] != "VALUE" and line[0..4] != "COUNT"
				return [line, values]
			end

			if line[0..4] == "VALUE"
				resp, flags, count = line.split()
				flags = flags.to_i
				count = count.to_i
			elsif line[0..4] == "COUNT"
				cmd, count = line.split("=")
				return [cmd, count.to_i]
			end
			
			for i in 0..count-1
				line = @handle.readline()
				if type == "bop" # bop get
					bkey, eflag, length_buf = line.split(" ", 3)

					if isdigit(eflag) # eflag not exist
						length = eflag
						eflag = nil
						buf = length_buf
					else
						length, buf = length_buf.split(" ", 2)
					end

					if isdigit(bkey)
						bkey = bkey.to_i
					end

					val = self.transcoder.decode(flags, buf)
					values[bkey] = [eflag, val]
				elsif type == "lop"
					length, buf = line.split(" ", 2)
					val = self.transcoder.decode(flags, buf)
					values.push(val)
				else # sop
					length, buf = line.split(" ", 2)
					val = self.transcoder.decode(flags, buf)
					values.add(val)
				end
			end
		end

		return nil
	end

			
	def _decode_bop_mget()
		values = {}
		missed_keys = []

		while true
			line = @handle.readline()
			if line[0..10] == "MISSED_KEYS"
				dummy, count = line.split(" ")
				count = count.to_i
				for i in 0..count-1
					line = @handle.readline()
					missed_keys.push(line)
				end

				redo
			end

			if line[0..4] != "VALUE" and line[0..4] != "COUNT"
				return [line, values, missed_keys]
			end

			ret = line.split()
			key = ret[1]
			status = ret[2]

			if status == "NOT_FOUND"
				missed_keys.push(key)
				redo
			end

			count = 0
			if len(ret) == 5
				flags = ret[3].to_i
				count = ret[4].to_i
			end

			val = {}
			for i in 0..count-1
				line = @handle.readline()
				element, bkey, eflag, length_buf = line.split(" ", 4)

				if isdigit(eflag) # eflag not exist
					length = eflag
					eflag = nil
					buf = length_buf
				else
					length, buf = length_buf.split(" ", 2) 
				end

				if isdigit(bkey)
					bkey = bkey.to_i
				end
	
				ret = self.transcoder.decode(flags, buf)
				val[bkey] = [eflag, ret]
			end

			values[key] = val
		end

		return nil
	end



	def _decode_bop_smget()
		values = []
		missed_keys = []

		while true
			line = @handle.readline()
			if line[0..10] == "MISSED_KEYS"
				dummy, count = line.split(" ")
				count = count.to_i
				for i in 0..count-1
					line = @handle.readline()
					missed_keys.push(line)
				end

				redo
			end

			if line[0..4] != "VALUE" and line[0..4] != "COUNT"
				return [line, values, missed_keys]
			end

			ret = line.split()
			count = ret[1].to_i
			
			for i in 0..count-1
				line = @handle.readline()
				key, flags, bkey, eflag, length_buf = line.split(" ", 5)

				if isdigit(eflag)  # eflag not exist
					length = eflag
					eflag = nil
					buf = length_buf
				else
					length, buf = length_buf.split(" ", 2)
				end

				if isdigit(bkey)
					bkey = bkey.to_i
				end
	
				val = self.transcoder.decode(flags.to_i, buf)
				values.push([bkey, key, eflag, val])
			end
		end

		return nil
	end
end

class EflagFilter
	def initialize(expr = nil)
		@lhs_offset = 0
		@bit_op = nil
		@bit_rhs = nil
		@comp_op = nil
		@comp_rhs = nil

		if expr != nil
			self._parse(expr)
		end
	end

	def get_expr()
		expr = ""
		if @lhs_offset != nil
			expr += "%d" % @lhs_offset

			if @bit_op and @bit_rhs
				expr += " %s %s" % [@bit_op, @bit_rhs]
			end

			if @comp_op and @comp_rhs
				expr += " %s %s" % [@comp_op, @comp_rhs]
			end
		end

		return expr			
	end

	def _parse(expr)
		re_expr = /EFLAG[ ]*(\[[ ]*([0-9]*)[ ]*\:[ ]*\])?[ ]*(([\&\|\^])[ ]*(0x[0-9a-fA-F]+))?[ ]*(==|\!=|<|>|<=|>=)[ ]*(0x[0-9a-fA-F]+)/

		match = re_expr.match(expr)
		if match == nil
			raise FilterInvalid()
		end
			
		# ( dummy, dummy, lhs_offset, dummy, bit_op, bit_rhs, comp_op, comp_rhs )
		g = match;
		@lhs_offset = g[2]
		@bit_op = g[4]
		@bit_rhs = g[5]
		@comp_op = g[6]
		@comp_rhs = g[7]

		if @lhs_offset == nil
			@lhs_offset = 0
		else
			@lhs_offset = @lhs_offset.to_i
		end

		if @comp_op == "=="
			@comp_op = "EQ"
		elsif @comp_op == "!="
			@comp_op = "NE"
		elsif @comp_op == "<"
			@comp_op = "LT"
		elsif @comp_op == "<="
			@comp_op = "LE"
		elsif @comp_op == ">"
			@comp_op = "GT"
		elsif @comp_op == ">="
			@comp_op = "GE"
		end
	end
end


class ArcusMCSelect
	def initialize()
		@sock_node_map = {}
	end

	def run()
		arcuslog(self, "select start")

		while true
			rdfs = @sock_node_map.keys

			rs, ws, es = IO.select(rdfs, nil, rdfs, 0.1)

			if ArcusMCNode.shutdown == true
				arcuslog(self, "select out")
				return
			end

			if rs == nil
				rs = []
			end

			if es == nil
				es = []
			end

			for rd in rs
				node = @sock_node_map[rd]
				node.do_op()
			end

			for ed in es
				node = @sock_node_map[ed]
				node.disconnect()
				@sock_node_map.delete(ed)
			end
		end
	end

	def register_node(node)
		arcuslog(self, 'regist node: ', node.get_fileno(), node)
		@sock_node_map[node.get_fileno()] = node
	end
end
		



class ArcusMCPoll
	def initialize()
		@epoll = Select::Epoll.new
		@sock_node_map = {}
	end

	def run()
		arcuslog(self, "epoll start")

		while true
			fds = []
			events = []
			@epoll.poll(2, fds, events)
			
			if ArcusMCNode.shutdown == true
				arcuslog(self, "epoll out")
				return
			end

			for i in (0..len(fds)-1)
				if events[i] & Select::EPOLLIN > 0
					node = @sock_node_map[fds[i]]
					node.do_op()
				end

				if events[i] & Select::EPOLLHUP > 0
					puts('EPOLL HUP')
					@epoll.unregister(fds[i])

					node = @sock_node_map[fds[i]]
					node.disconnect()
					@sock_node_map.delete(fds[i])
				end
			end
		end
	end

	def register_node(node)
		@epoll.register(node.get_fileno(), Select::EPOLLIN | Select::EPOLLHUP)
		arcuslog(self, 'regist node: ', node.get_fileno(), node)
		@sock_node_map[node.get_fileno()] = node
	end
end
		
		
class ArcusMCWorker
	attr_reader :q
	attr_writer :q

	def initialize()
		@q = Queue.new
		@poll = ArcusMCPoll.new()
		#@poll = ArcusMCSelect.new()
		Thread.new { 
			#begin
				@poll.run
			#rescue
			#	pp $!
			#end
		}
	end

	def run()
		arcuslog(self, "worker start")

		while true
			op = @q.pop()
			arcuslog(self, "worker wakeup %p" % op)

			if ArcusMCNode.shutdown == true
				arcuslog(self, "worker done")
				return
			end


			if op == nil # maybe shutdown
				arcuslog(self, "invalid op, shutdown?")
				redo
			end
		
			arcuslog(self, "get operation %s(%s) from %s" % [op.request, op.callback, op.node])
			node = op.node
			node.process_request(op.request)
		end
	end


	def register_node(node)
		@poll.register_node(node)
	end
end
		


class ArcusMCNodeAllocator
	def initialize(transcoder)
		@transcoder = transcoder
		ArcusMCNode.worker(ArcusMCWorker.new)
		Thread.new {
			#begin
				 ArcusMCNode.worker.run
			#rescue
			#	pp $!
			#end
		}
	end
		

	def alloc(addr, name)
		ret = ArcusMCNode.new(addr, name, @transcoder)
		ArcusMCNode.worker.register_node(ret)
		return ret
	end
end

