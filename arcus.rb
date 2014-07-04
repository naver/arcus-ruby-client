
require 'thread'
require 'zk'
require 'digest/md5'
require 'zlib'
require 'bisect'
require 'set'
require 'time'
require 'Select'

$g_log = false

def len(p)
	return p.length
end


def enable_log(flag = true)
	$g_log = flag
end

def arcuslog(caller, *param)
	if $g_log
		str = ''
		if caller
			str = ''
		end

		for p in param
			str = str + p.to_s
		end

		puts(str)
	end
end

class ArcusException < Exception
	attr_reader :msg
	attr_writer :msg

	def initialize(msg)
		@msg = msg
	end
end



class ArcusProtocolException < ArcusException
	def initialize(msg)
		@msg = msg
	end
end



class ArcusNodeException < ArcusException
	def initialize(msg)
		@msg = msg
	end
end

class ArcusNodeSocketException < ArcusNodeException
	def initialize(msg)
		@msg = msg
	end
end

class ArcusNodeConnectionException < ArcusNodeException
	def initialize(msg)
		@msg = msg
	end
end




class ArcusListException < ArcusException
	def initialize(msg)
		@msg = msg
	end
end




class CollectionException < ArcusException
	def initialize(msg)
		@msg = msg
	end
end

class CollectionType < CollectionException
	def initialize(msg='collection type mismatch')
		@msg = msg
	end
end

class CollectionExist < CollectionException
	def initialize(msg='collection already exits')
		@msg = msg
	end
end

class CollectionIndex < CollectionException
	def initialize(msg='invalid index or range')
		@msg = msg
	end
end

class CollectionOverflow < CollectionException
	def initialize(msg='collection overflow')
		@msg = msg
	end
end

class CollectionUnreadable < CollectionException
	def initialize(msg='collection is unreadable')
		@msg = msg
	end
end

class CollectionHexFormat < CollectionException
	def initialize(msg='invalid hex string format')
		@msg = msg
	end
end

class FilterInvalid < CollectionException
	def initialize(msg='invalid fiter expression')
		@msg = msg
	end
end



# TODO: pickle in ruby
class ArcusTranscoder
	# primitive type
	FLAG_MASK=0xff00
	FLAG_STRING=0
	FLAG_BOOLEAN=(1<<8)
	FLAG_INTEGER=(2<<8)	# decode only (for other client)
	FLAG_LONG=(3<<8) 
	FLAG_DATE=(4<<8)
	FLAG_BYTE=(5<<8)	# decode only (for other client)
	FLAG_FLOAT=(6<<8)	# decode only (for other client)
	FLAG_DOUBLE=(7<<8)
	FLAG_BYTEARRAY=(8<<8)

	# general case
	FLAG_SERIALIZED = 1	# used at java
	FLAG_COMPRESSED = 2
	FLAG_PICKLE = 4

	def initialize()
		@min_compress_len = 0	
	end

	def encode(val)
		flags = 0
		if val.is_a?(String)
			ret = val
		elsif val.is_a?(TrueClass)
			flags |= FLAG_BOOLEAN
			if val
				ret = [1].pack('c')
			else
				ret = [0].pack('c')
			end
			
		elsif val.is_a?(Fixnum)
			flags |= FLAG_LONG
			ret = [val].pack('q>')
		elsif val.is_a?(Float)
			flags |= FLAG_DOUBLE
			ret = [val].pack('G')
		elsif val.is_a?(DateTime)
			flags |= FLAG_DATE
			ret = val.strftime('%Q').to_i
			ret = [ret].pack('q>')
		elsif val.is_a?(bytes)
			flags |= FLAG_BYTEARRAY
			ret = val
		end

		lv = len(ret)
		if @min_compress_len and lv > @min_compress_len
			comp_val = Zlib::Deflate.deflate(ret)
			if len(comp_val) < lv
				flags |= FLAG_COMPRESSED
				ret = comp_val
			end
		end

		return [flags, len(ret), ret]
	end

	def decode(flags, buf)
		if flags & FLAG_COMPRESSED != 0
			buf = Zlib::Inflate.inflate(buf)
		end

		flags = flags & FLAG_MASK

		if  flags == 0
			val = buf;
		elsif flags == FLAG_BOOLEAN
			val = buf.unpack('c')[0]
			if val == 1
				val = true
			else
				val = false
			end

		elsif flags == FLAG_INTEGER or flags == FLAG_LONG or flags == FLAG_BYTE
			val = 0
			l = len(buf)
			for i in 0..l-1
				val = val + (buf[i].ord << (8*(l-i-1)))
			end

		elsif flags == FLAG_DATE
			val = 0
			l = len(buf)
			for i in 0..l-1
				val = val + (buf[i].ord << (8*(l-i-1)))
				puts val
			end

			val = DateTime.strptime(val.to_s, '%Q')
		elsif flags == FLAG_FLOAT
			val = buf.unpack('g')[0]
		elsif flags == FLAG_DOUBLE
			val = buf.unpack('G')[0]
		elsif flags == FLAG_BYTEARRAY
			val = buf
		else
			arcuslog('unknown flags on get: %x\n' % flags)
		end

		return val
	end
end



class ArcusKetemaHash
	attr_reader :per_node, :per_hash
	attr_writer :per_node, :per_hash

	def initialize
		# config
		@per_node = 40
		@per_hash = 4
	end

	def hash(addr)
		ret = []
		for i in 0..@per_node-1
			ret = ret + self.__hash(addr + ('-%d' % i))
		end

		return ret
	end

	def __hash(input)
		r = Digest::MD5.digest(input);
		ret = []

		for i in 0..@per_hash-1
			hash = (r[3 + i*4].ord << 24) | (r[2 + i*4].ord << 16) | (r[1 + i*4].ord << 8) | r[0 + i*4].ord
			ret.push(hash)
		end

		return ret
	end
end



class ArcusPoint
	attr_reader :hash, :node
	attr_writer :hash, :node

	def initialize(hash, node)
		@hash = hash
		@node = node
	end

	def <=>(rhs)
		return @hash <=> rhs.hash
	end

	def <(rhs)
		return @hash < rhs.hash;
	end

	def <=(rhs)
		return @hash <= rhs.hash;
	end

	def ==(rhs)
		return @hash == rhs.hash;
	end

	def !=(rhs)
		return @hash != rhs.hash;
	end

	def >(rhs)
		return @hash > rhs.hash;
	end

	def >=(rhs)
		return @hash >= rhs.hash;
	end

	def to_s
		return '(%d:%s)' % [@hash, @node]
	end
end



class ArcusLocator
	attr_reader :hash_method, :lock, :node_list, :attr_node_map, :node_allocator
	attr_writer :hash_method, :lock, :node_list, :attr_node_map, :node_allocator

	def initialize(node_allocator)
		# config 
		@hash_method = ArcusKetemaHash.new

		# init
		@lock = Mutex.new
		@node_list = []
		@addr_node_map = {}
		@node_allocator = node_allocator
	end

	def connect(addr, code)
		# init zookeeper
		arcuslog(self, 'zoo keeper init')

		@zk = ZK.new(addr)
		@zoo_path = '/arcus/cache_list/' + code

		arcuslog(self, 'zoo keeper get path: ' + @zoo_path)
		data, stat = @zk.get(@zoo_path)
		arcuslog(self, 'zoo keeper node info with stat: ', data, stat)

		children = @zk.children(@zoo_path, :watch => true)
		self.hash_nodes(children)
	end

	def disconnect()
		for node in @addr_node_map.values()
			node.disconnect_all()
		end

		@addr_node_map = {}
		@node_list = []
		@zk.close()
	end

	def hash_nodes(children)
		arcuslog(self, 'hash_nodes with children: ', children)

		@lock.lock()
 
		# clear first
		@node_list = []
		for node in @addr_node_map.values()
			node.in_use = false
		end
			
		# update live nodes
		for child in children
			lst = child.split('-')
			addr = lst[0]
			name = lst[1]

			if @addr_node_map.has_key?(addr)
				@addr_node_map[addr].in_use = true
				node = @addr_node_map[addr];
			else
				# new node
				node = @node_allocator.alloc(addr, name)
				@addr_node_map[addr] = node
				@addr_node_map[addr].in_use = true
			end

			hash_list = @hash_method.hash(node.addr)
			#arcuslog(self, 'hash_lists of node(%s): %s' % [node.addr, hash_list])

			for hash in hash_list
				point = ArcusPoint.new(hash, node)
				@node_list.push(point)
			end
		end

		# sort list
		@node_list.sort()
		#arcuslog(self, 'sorted node list', @node_list)

		# disconnect dead node
		for addr, node in @addr_node_map
			if node.in_use == false
				node.disconnect_all()
				@addr_node_map.delete(addr)
			end
		end

		@lock.unlock()
	end

	def watch_children(event)
		arcuslog(self, 'watch children called: ', event)

		# rehashing
		children = @zk.get_children(event.path)
		self.hash_nodes(children)
	end

	def get_node(key)
		hash = self.__hash_key(key)

		@lock.lock()
		idx = Bisect.bisect(@node_list, ArcusPoint.new(hash, nil))

		# roll over
		if idx > len(@node_list)
			idx = 0
		end

		point = @node_list[idx]
		@lock.unlock()

		return point.node
	end

	def __hash_key(key)
		bkey = key;

		r = Digest::MD5.digest(bkey);
		ret = r[3].ord << 24 | r[2].ord << 16 | r[1].ord << 8 | r[0].ord
		return ret
	end
end


class Arcus
	attr_reader :locator
	attr_writer :locator

	def initialize(locator)
		@locator = locator
	end

	def connect(addr, code)
		@locator.connect(addr, code)
	end

	def disconnect()
		@locator.disconnect()
	end

	def set(key, val, exptime=0)
		node = @locator.get_node(key)
		return node.set(key, val, exptime)
	end

	def get(key)
		node = @locator.get_node(key)
		return node.get(key)
	end

	def gets(key)
		node = @locator.get_node(key)
		return node.gets(key)
	end

	def incr(key, val=1)
		node = @locator.get_node(key)
		return node.incr(key, val)
	end

	def decr(key, val=1)
		node = @locator.get_node(key)
		return node.decr(key, val)
	end

	def delete(key)
		node = @locator.get_node(key)
		return node.delete(key)
	end

	def add(key, val, exptime=0)
		node = @locator.get_node(key)
		return node.add(key, val, exptime)
	end

	def append(key, val, exptime=0)
		node = @locator.get_node(key)
		return node.append(key, val, exptime)
	end

	def prepend(key, val, exptime=0)
		node = @locator.get_node(key)
		return node.prepend(key, val, exptime)
	end

	def replace(key, val, exptime=0)
		node = @locator.get_node(key)
		return node.replace(key, val, exptime)
	end

	def cas(key, val, cas_id, exptime=0)
		node = @locator.get_node(key)
		return node.cas(key, val, cas_id, time)
	end

	def lop_create(key, flags, exptime=0, noreply=false, attr_map=nil)
		node = @locator.get_node(key)
		return node.lop_create(key, flags, exptime, noreply, attr_map)
	end
		
	def lop_insert(key, index, value, noreply=false, pipe=false, attr_map=nil)
		node = @locator.get_node(key)
		return node.lop_insert(key, index, value, noreply, pipe, attr_map)
	end
		
	def lop_get(key, range, delete=false, drop=false)
		node = @locator.get_node(key)
		return node.lop_get(key, range, delete, drop)
	end

	def lop_delete(key, range, drop=false, noreply=false, pipe=false)
		node = @locator.get_node(key)
		return node.lop_delete(key, range, drop, noreply, pipe)
	end

	def sop_create(key, flags, exptime=0, noreply=false, attr_map=nil)
		node = @locator.get_node(key)
		return node.sop_create(key, flags, exptime, noreply, attr_map)
	end
		
	def sop_insert(key, value, noreply=false, pipe=false, attr_map=nil)
		node = @locator.get_node(key)
		return node.sop_insert(key, value, noreply, pipe, attr_map)
	end
		
	def sop_get(key, count=0, delete=false, drop=false)
		node = @locator.get_node(key)
		return node.sop_get(key, count, delete, drop)
	end

	def sop_delete(key, value, drop=false, noreply=false, pipe=false)
		node = @locator.get_node(key)
		return node.sop_delete(key, value, drop, noreply, pipe)
	end

	def sop_exist(key, value, pipe=false)
		node = @locator.get_node(key)
		return node.sop_exist(key, value, pipe)
	end

	def bop_create(key, flags, exptime=0, noreply=false, attr_map=nil)
		node = @locator.get_node(key)
		return node.bop_create(key, flags, exptime, noreply, attr_map)
	end
		
	def bop_insert(key, bkey, value, eflag=nil, noreply=false, pipe=false, attr_map=nil)
		node = @locator.get_node(key)
		return node.bop_insert(key, bkey, value, eflag, noreply, pipe, attr_map)
	end

	def bop_upsert(key, bkey, value, eflag=nil, noreply=false, pipe=false, attr_map=nil)
		node = @locator.get_node(key)
		return node.bop_upsert(key, bkey, value, eflag, noreply, pipe, attr_map)
	end

	def bop_update(key, bkey, value, eflag=nil, noreply=false, pipe=false, attr_map=nil)
		node = @locator.get_node(key)
		return node.bop_update(key, bkey, value, eflag, noreply, pipe, attr_map)
	end
		
	def bop_get(key, range, filter=nil, delete=false, drop=false)
		node = @locator.get_node(key)
		return node.bop_get(key, range, filter, delete, drop)
	end

	def bop_delete(key, range, filter=nil, count=nil, drop=false, noreply=false, pipe=false)
		node = @locator.get_node(key)
		return node.bop_delete(key, range, filter, count, drop, noreply, pipe)
	end

	def bop_count(key, range, filter=nil)
		node = @locator.get_node(key)
		return node.bop_count(key, range, filter)
	end

	def bop_incr(key, bkey, value, noreply=false, pipe=false)
		node = @locator.get_node(key)
		return node.bop_incr(key, bkey, value, noreply, pipe)
	end

	def bop_decr(key, bkey, value, noreply=false, pipe=false)
		node = @locator.get_node(key)
		return node.bop_incr(key, bkey, value, noreply, pipe)
	end

	def bop_mget(key_list, range, filter=nil, offset=nil, count=50)
		nodes = {}

		for key in key_list
			node = @locator.get_node(key)
			if !nodes.has_key?(node)
				nodes[node] = [key]
			else
				nodes[node].push(key)
			end
		end
				
		op_list = ArcusOperationList.new('bop mget')
		nodes.each do | node, keys |
			op = node.bop_mget(keys, range, filter, offset, count)
			op_list.add_op(op)
		end

		return op_list
	end


	def bop_smget(key_list, range, filter=nil, offset=nil, count=2000)
		nodes = {}

		for key in key_list
			node = @locator.get_node(key)
			if !nodes.has_key?(node)
				nodes[node] = [key]
			else
				nodes[node].push(key)
			end
		end
				
		op_list = ArcusOperationList.new('bop smget')
		nodes.each do | node, keys |
			op = node.bop_smget(keys, range, filter, offset, count)
			op_list.add_op(op)
		end

		return op_list
	end

	def list_alloc(key, flags, exptime=0, cache_time=0)
		self.lop_create(key, flags, exptime)
		return self.list_get(key, cache_time)
	end

	def list_get(key, cache_time=0)
		return  ArcusList.new(self, key, cache_time)
	end
		
	def set_alloc(key, flags, exptime=0, cache_time=0)
		self.sop_create(key, flags, exptime)
		return self.set_get(key, cache_time)
	end

	def set_get(key, cache_time=0)
		return  ArcusSet.new(self, key, cache_time)
	end
end
		

class ArcusOperation
	attr_reader :node, :request, :callback, :q, :result, :invalid, :noreply, :pipe
	attr_writer :node, :request, :callback, :q, :result, :invalid, :noreply, :pipe

	def initialize(node, request, callback)
		@node = node
		@request = request
		@callback = callback
		@q = Queue.new
		@result = nil
		@invalid = false

		@noreply = false
		@pipe = false
	end

	def to_s
		return '<%p result: %s>' % [self, self.get_result().to_s]
	end

	def has_result()
		return (@result != nil or @q.empty() == false)
	end

	def set_result(result)
		@q.push(result)
	end

	def set_invalid()
		if self.has_result()
			return false
		end

		@invalid = true
		@q.push(nil)	# wake up blocked callers.
		return true
	end

	def get_result(timeout=0)
		if @result != nil
			return @result
		end

		if timeout > 0
			result = @q.pop(false, timeout)
		else
			result = @q.pop()
		end

		if result == nil and @invalid == true
			raise ArcusNodeConnectionException('current async result is unavailable because Arcus node is disconnected now')
		end

		@result = result
		return result
	end
end




class ArcusOperationList
	def initialize(cmd)
		@ops = []
		@cmd = cmd
		@result = nil
		@missed_key = nil
		@invalid = false

		@noreply = false
		@pipe = false
	end

	def to_s
		return '<ArcusOperationList[%s] result: %s>' % [hex(id(self)), repr(self.get_result())]
	end

	def add_op(op)
		@ops.push(op)
	end

	def has_result()
		if @result != nil
			return true
		end

		for a in ops
			if a.has_result() == false
				return false
			end
		end

		return true
	end

	def set_result(result)
		assert false
		pass
	end

	def set_invalidate()
		if self.has_result()
			return false # already done
		end

		@invalid = true

		# invalidate all ops and wake up blockers.
		for a in ops
			a.set_invalidate()
		end

		return true
	end

	def get_missed_key(timeout=0)
		if @missed_key != nil
			return @missed_key
		end

		self.get_result(timeout)
		return @missed_key
	end

	def get_result(timeout=0)
		if @result != nil
			return @result
		end

		tmp_result = []
		missed_key = []
		if (timeout > 0)
			start_time = Time.now()
			end_time = start_tume + timeout

			for a in @ops
				curr_time = Time.now()
				remain_time = end_time - curr_time
				if remain_time < 0
					raise Queue.Empty()
				end

				ret, miss = a.get_result(remain_time)
				tmp_result.push(ret)
				missed_key += miss
			end
		else
			for a in @ops
				ret, miss = a.get_result()
				tmp_result.push(ret)
				missed_key += miss
			end
		end

		if @cmd == 'bop mget'
			result = {}
			for a in tmp_result
				result.update(a)
			end

		else # bop smget
			length = len(tmp_result)
			
			# empty
			if length <= 0
				return []
			end
				
			# merge sort
			result = []
			while true
				# remove empty list
				while len(tmp_result[0]) == 0
					tmp_result.shift()
					if len(tmp_result) == 0 # all done
						if @result == nil and @invalid == true
							raise ArcusNodeConnectionException('current async result is unavailable because Arcus node is disconnected now')
						end

						missed_key.sort()
						@result = result
						@missed_key = missed_key
						return @result
					end
				end

				min = tmp_result[0][0][0]
				idx = 0
				for i in (0..len(tmp_result)-1)
					if len(tmp_result[i]) and tmp_result[i][0][0] < min
						min = tmp_result[i][0]
						idx = i
					end
				end

				result.push(tmp_result[idx].shift())
			end
		end
		

		if @result == nil and @invalid == true
			raise ArcusNodeConnectionException('current async result is unavailable because Arcus node is disconnected now')
		end

		missed_key.sort()
		@result = result
		@missed_key = missed_key
		return @result
	end
end




class ArcusList
	def initialize(arcus, key, cache_time=0)
		@arcus = arcus
		@key = key
		@cache_time = cache_time

		if cache_time > 0
			begin
				@cache = @arcus.lop_get(@key, [0, -1]).get_result()
			rescue
				@cache = []
			end
		else
			@cache = nil
		end

		@next_refresh = Time.now() + cache_time
	end

	def length()
		if @cache != nil
			if Time.now() < @next_refresh
				return len(@cache)
			else
				@cache = @arcus.lop_get(@key, [0, -1]).get_result()
				@next_refresh = Time.now() + @cache_time
				return len(@cache)
			end
		else
			return len(@arcus.lop_get(@key, [0, -1]).get_result())
		end
	end

	def each
		if @cache != nil
			if Time.now() < @next_refresh
				return @cache.each { |item| yield item }
			else
				@cache = @arcus.lop_get(@key, [0, -1]).get_result()
				@next_refresh = Time.now() + @cache_time
				return @cache.each { |item| yield item }
			end
		else
			return @arcus.lop_get(@key, [0, -1]).get_result().each { |item| yield item }
		end
	end

	def ==(rhs)
		if @cache != nil
			if Time.now() < @next_refresh
				return @cache == rhs
			else
				@cache = @arcus.lop_get(@key, [0, -1]).get_result()
				@next_refresh = Time.now() + @cache_time
				return @cache == rhs
			end
		else
			return @arcus.lop_get(@key, [0, -1]).get_result() == rhs
		end
	end
		
	def !=(rhs)
		if @cache != nil
			if Time.now() < @next_refresh
				return @cache != rhs
			else
				@cache = @arcus.lop_get(@key, [0, -1]).get_result()
				@next_refresh = Time.now() + @cache_time
				return @cache != rhs
			end
		else
			return @arcus.lop_get(@key, [0, -1]).get_result() != rhs
		end
	end

	def <=(rhs)
		if @cache != nil
			if Time.now() < @next_refresh
				return @cache <= rhs
			else
				@cache = @arcus.lop_get(@key, [0, -1]).get_result()
				@next_refresh = Time.now() + @cache_time
				return @cache <= rhs
			end
		else
			return @arcus.lop_get(@key, [0, -1]).get_result() <= rhs
		end
	end

	def <(rhs)
		if @cache != nil
			if Time.now() < @next_refresh
				return @cache < rhs
			else
				@cache = @arcus.lop_get(@key, [0, -1]).get_result()
				@next_refresh = Time.now() + @cache_time
				return @cache < rhs
			end
		else
			return @arcus.lop_get(@key, [0, -1]).get_result() < rhs
		end
	end

	def >=(rhs)
		if @cache != nil
			if Time.now() < @next_refresh
				return @cache >= rhs
			else
				@cache = @arcus.lop_get(@key, [0, -1]).get_result()
				@next_refresh = Time.now() + @cache_time
				return @cache >= rhs
			end
		else
			return @arcus.lop_get(@key, [0, -1]).get_result() >= rhs
		end
	end

	def >(rhs)
		if @cache != nil
			if Time.now() < @next_refresh
				return @cache > rhs
			else
				@cache = @arcus.lop_get(@key, [0, -1]).get_result()
				@next_refresh = Time.now() + @cache_time
				return @cache > rhs
			end
		else
			return @arcus.lop_get(@key, [0, -1]).get_result() > rhs
		end
	end
			
	def [](index)
		if @cache != nil
			if Time.now() < @next_refresh
				return @cache[index]
			else
				@cache = @arcus.lop_get(@key, [0, -1]).get_result()
				@next_refresh = Time.now() + @cache_time
				return @cache[index]
			end
		else
			if index.is_a?(Range)
				start = index.begin
				stop = index.end

				if start == nil
					start = 0
				end

				if stop == nil
					stop = -1
				end

				begin 
					return @arcus.lop_get(@key, [start, stop]).get_result()
				rescue
					return []
				end
			else
				ret = @arcus.lop_get(@key, index).get_result()
				if len(ret) == 0
					raise IndexError('lop index out of range')
				end

				return ret[0]
			end
		end
	end
				
			
	def []=(value)
		raise ArcusListException('list set is not possible')
	end

	def delete_at(index)
		if @cache != nil
			@cache.delete_at[index]
		end

		if index.is_a?(Range)
			start = index.begin
			stop = index.end

			if start == nil
				start = 0
			end

			if stop == nil
				stop = -1
			end

			return @arcus.lop_delete(@key, [start, stop]).get_result()
		else
			return @arcus.lop_delete(@key, index).get_result()
		end
	end

	def insert(index, value)
		if @cache != nil
			@cache.insert(index, value)
		end

		return @arcus.lop_insert(@key, index, value).get_result()
	end
			
	def push(value)
		if @cache != nil
			@cache.push(value)
		end

		return @arcus.lop_insert(@key, -1, value).get_result()
	end

	def invalidate()
		if @cache != nil
			begin
				@cache = @arcus.lop_get(@key, [0, -1]).get_result()
			rescue
				@cache = []
			end

			@next_refresh = Time.now() + @cache_time
		end
	end
			
	def to_s()
		if @cache != nil
			if Time.now() < @next_refresh
				return @cachae.to_s
			else
				@cache = @arcus.lop_get(@key, [0, -1]).get_result()
				@next_refresh = Time.now() + @cache_time
				return @cache.to_s
			end
		end

		begin
			ret = @arcus.lop_get(@key, [0, -1]).get_result()
		rescue
			ret = [] # not found?
		end

		return  ret.to_s
	end
end




class ArcusSet
	def initialize(arcus, key, cache_time=0)
		@arcus = arcus
		@key = key
		@cache_time = cache_time

		if cache_time > 0
			begin
				@cache = @arcus.sop_get(@key).get_result()
			rescue
				@cache = Set.new
			end
		else
			@cache = nil
		end

		@next_refresh = Time.now() + cache_time
	end

	def length()
		if @cache != nil
			if Time.now() < @next_refresh
				return len(@cache)
			else
				@cache = @arcus.sop_get(@key).get_result()
				@next_refresh = Time.now() + @cache_time
				return len(@cache)
			end
		else 
			return len(@arcus.sop_get(@key).get_result())
		end
	end

	def include?(value)
		if @cache != nil and Time.now() < @next_refresh
			return @cache.include?(value) # do not fetch all for cache when time over
		end

		return @arcus.sop_exist(@key, value).get_result()
	end

	def each
		if @cache != nil
			if Time.now() < @next_refresh
				return @cache.each { |item| yield item }
			else
				@cache = @arcus.sop_get(@key).get_result()
				@next_refresh = Time.now() + @cache_time
				return @cache.each { |item| yield item }
			end
		else
			return @arcus.sop_get(@key).get_result().each { |item| yield item }
		end
	end
			
	def add(value)
		if @cache != nil
			@cache[value] = True
		end

		return @arcus.sop_insert(@key, value).get_result()
	end

	def invalidate()
		if @cache != nil
			begin
				@cache = @arcus.sop_get(@key).get_result()
			rescue
				@cache = Set.new
			end

			@next_refresh = Time.now() + @cache_time
		end
	end
			
	def to_s()
		if @cache != nil
			if Time.now() < @next_refresh
				return @cache.to_s
			else
				@cache = @arcus.sop_get(@key).get_result()
				@next_refresh = Time.now() + @cache_time
				return @cache.to_s
			end
		end

		begin
			ret = @arcus.sop_get(@key).get_result()
		rescue
			ret = Set.new
		end

		return  ret.to_s
	end
end

		





