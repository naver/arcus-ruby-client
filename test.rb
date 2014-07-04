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

#
# ARGV[0] : connection url for Arcus cloud (zookeeper address:port)
# ARGV[1] : Arcus cloud service code
#
# USAGE: ruby test.rb your.arcuscloud.com:11223 service_code
#

require 'set'
require 'date'

require './arcus'
require './arcus_mc_node'

#enable_log()



def assert(a)
	if a == false
		puts('[Assert] Test Failed')
		exit()
	end
end

# client which use arcus memcached node & default arcus transcoder
allocator = ArcusMCNodeAllocator.new(ArcusTranscoder.new)
client = Arcus.new(ArcusLocator.new(allocator))

puts('### connect to client')
client.connect(ARGV[0], ARGV[1])

#####################################################################################################
#
# TEST 1: primitive type
#
#####################################################################################################
ret = client.set('test:string1', 'test...', 3)
puts(ret.get_result())
assert ret.get_result() == true

ret = client.get('test:string1')
puts(ret.get_result())
assert ret.get_result() == 'test...'

ret = client.set('test:string2', 'test...2', 3)
puts(ret.get_result())
assert ret.get_result() == true

ret = client.get('test:string2')
puts(ret.get_result())
assert ret.get_result() == 'test...2'

ret = client.set('test:int', 1, 3)
puts(ret.get_result())
assert ret.get_result() == true

ret = client.get('test:int')
puts(ret.get_result())
assert ret.get_result() == 1

ret = client.set('test:float', 1.2, 3)
puts(ret.get_result())
assert ret.get_result() == true

ret = client.get('test:float')
puts(ret.get_result())
assert ret.get_result() == 1.2

ret = client.set('test:bool', true, 3)
puts(ret.get_result())
assert ret.get_result() == true

ret = client.get('test:bool')
puts(ret.get_result())
assert ret.get_result() == true

now = DateTime.now()
ret = client.set('test:date', now, 3)
puts(ret.get_result())
assert ret.get_result() == true

ret = client.get('test:date')
puts(ret.get_result())
puts(now)

now = now.strftime('%Q').to_i
ret = ret.get_result().strftime('%Q').to_i
assert ret - now < 1000 # msec

#
# unlike python,
# There's no primitive type for byte array in Ruby.
#
#ret = client.set('test:bytearray', b'bytes array', 3)
#puts(ret.get_result())
#assert ret.get_result() == true
#
#ret = client.get('test:bytearray')
#puts(ret.get_result())
#assert ret.get_result() == b'bytes array'


ret = client.set('test:incr', '1', 3)
puts(ret.get_result())
assert ret.get_result() == true

ret = client.incr('test:incr', 10)
puts(ret.get_result())
assert ret.get_result() == 11

ret = client.decr('test:incr', 3)
puts(ret.get_result())
assert ret.get_result() == 11-3

ret = client.decr('test:incr', 100)
puts(ret.get_result())
assert ret.get_result() == 0 # minimum value is 0





#####################################################################################################
#
# TEST 2: list
#
#####################################################################################################
ret = client.lop_create('test:list_1', ArcusTranscoder::FLAG_STRING, 3)
puts(ret.get_result())
assert ret.get_result() == true

items = ['item 1', 'item 2', 'item 3', 'item 4', 'item 5', 'item 6']

for item in items
	ret = client.lop_insert('test:list_1', -1, item)
	puts(ret.get_result())
	assert ret.get_result() == true
end

ret = client.lop_get('test:list_1', [0, -1])
puts(ret.get_result())
assert ret.get_result() == items

ret = client.lop_get('test:list_1', [2, 4])
puts(ret.get_result())
assert ret.get_result() == items[2..4]

ret = client.lop_get('test:list_1', [1, -2])
puts(ret.get_result())
assert ret.get_result() == items[1..-2]


#####################################################################################################
#
# TEST 3: set
#
#####################################################################################################
ret = client.sop_create('test:set_1', ArcusTranscoder::FLAG_STRING, 3)
puts(ret.get_result())
assert ret.get_result() == true

items = ['item 1', 'item 2', 'item 3', 'item 4', 'item 5', 'item 6']
set_items = Set.new
for item in items
	set_items.add(item)
end

for item in set_items
	ret = client.sop_insert('test:set_1', item)
	puts(ret.get_result())
	assert ret.get_result() == true
end

ret = client.sop_get('test:set_1')
puts(ret.get_result())
assert ret.get_result() == set_items

for item in set_items
	ret = client.sop_exist('test:set_1', item)
	puts(ret.get_result())
	assert ret.get_result() == true
end

ret = client.sop_exist('test:set_1', 'item 100')
puts(ret.get_result())
assert ret.get_result() == false





#####################################################################################################
#
# TEST 4: btree
#
#####################################################################################################

def itoh(i)
	h = i.to_s(16)
	if len(h) % 2 == 1
		h = '0x0%s' % h.upcase
	else
		h = '0x%s' % h.upcase
	end

	return h
end



if false
	


# int key
ret = client.bop_create('test:btree_int', ArcusTranscoder::FLAG_INTEGER, 3)
puts (ret.get_result())
assert ret.get_result() == true

for i in (0..999)
	ret = client.bop_insert('test:btree_int', i, i, itoh(i))
	puts(ret.get_result())
	assert ret.get_result() == true
end

ret = client.bop_get('test:btree_int', [200, 400])
puts(ret.get_result())

result = ret.get_result()
for i in (200..399)
	assert result[i] == [itoh(i), i]
end

ret = client.bop_count('test:btree_int', [100, 199])
puts(ret.get_result())
assert ret.get_result() == 100





# hex key
ret = client.bop_create('test:btree_hex', ArcusTranscoder::FLAG_STRING, 3)
puts (ret.get_result())
assert ret.get_result() == true

for i in (0x10000.. 0x101ff)
	ret = client.bop_insert('test:btree_hex', itoh(i), 'bop item %d' % i, itoh(i))
	puts(ret.get_result())
	assert ret.get_result() == true
end

ret = client.bop_get('test:btree_hex', ['0x010050', '0x010150'])
puts(ret.get_result())

result = ret.get_result()
for i in (0x10050..0x1014f)
	assert result[itoh(i)] == [itoh(i), 'bop item %d' % i]
end




# eflag test

ret = client.bop_create('test:btree_eflag', ArcusTranscoder::FLAG_INTEGER, 3)
puts (ret.get_result())
assert ret.get_result() == true

for i in (0..999)
	ret = client.bop_insert('test:btree_eflag', i, i, itoh(i))
	puts(ret.get_result())
	assert ret.get_result() == true
end

ret = client.bop_get('test:btree_eflag', [200, 400], EflagFilter.new('EFLAG & 0x00ff == 0x0001'))
puts(ret.get_result())
result = ret.get_result()
assert result[257] == ['0x0101', 257]


ret = client.bop_get('test:btree_eflag', [200, 400], EflagFilter.new('EFLAG & 0x00ff > 0x0010'))
puts(ret.get_result())
result = ret.get_result()

for i in (200..400)
	if (len(itoh(i)) < 6)
		if result.include? i
			assert false
		end
		next
	end

	if (i & 0x00ff) <= 0x0010
		if result.include? i
			assert false
		end
		next
	end

	assert result[i] == [itoh(i), i]
end



end


#####################################################################################################
#
# TEST 5: btree mget, smget
#
#####################################################################################################
# int key
ret = client.bop_create('test:btree_1', ArcusTranscoder::FLAG_INTEGER, 3)
puts (ret.get_result())
assert ret.get_result() == true

for i in (0..999)
	ret = client.bop_insert('test:btree_1', i, i, itoh(i))
	puts(ret.get_result())
	assert ret.get_result() == true
end


ret = client.bop_create('test:btree_2', ArcusTranscoder::FLAG_INTEGER, 3)
puts (ret.get_result())
assert ret.get_result() == true

for i in (1000..1999)
	ret = client.bop_insert('test:btree_2', i, i, itoh(i))
	puts(ret.get_result())
	assert ret.get_result() == true
end

ret = client.bop_create('test:btree_3', ArcusTranscoder::FLAG_INTEGER, 3)
puts (ret.get_result())
assert ret.get_result() == true

for i in (2000..2999)
	ret = client.bop_insert('test:btree_3', i, i, itoh(i))
	puts(ret.get_result())
	assert ret.get_result() == true
end



ret = client.bop_mget(['test:btree_1', 'test:btree_2', 'test:btree_3', 'test:btree_4', 'test:btree_5'], [500, 2500])
print(ret.get_result())
print('\n');





ret = client.bop_smget(['test:btree_1', 'test:btree_2', 'test:btree_3', 'test:btree_4', 'test:btree_5'], [500, 2500])
print(ret.get_result())
print('\n');

result = ret.get_result()
missed_key = ret.get_missed_key()

idx = 500
for item in result
	assert item[0] == idx # bkey
	assert item[1][0..10] == 'test:btree_' # key
	assert item[2] == itoh(idx) # eflag
	assert item[3] == idx # value
	idx += 1
end
	
assert missed_key == ['test:btree_4', 'test:btree_5']





#####################################################################################################
#
# TEST 6: dynamic list
#
#####################################################################################################

arcus_list = client.list_alloc('test:arcus_list', ArcusTranscoder::FLAG_STRING, 3)
print (arcus_list)
assert arcus_list == []

items = ['item 1', 'item 2', 'item 3', 'item 4', 'item 5', 'item 6']

for item in items
	arcus_list.push(item)
end

print (arcus_list)
assert arcus_list == items
print (arcus_list[2..4])
assert arcus_list[2..4] == items[2..4]
print (arcus_list[0..2])
assert arcus_list[0..2] == items[0..2]
print (arcus_list[3..-1])
assert arcus_list[3..-1] == items[3..-1]


print('## for loop test')
idx = 0
for a in arcus_list
	puts(a)
	assert a == items[idx]
	idx += 1
end


# cached ArcusList Test
arcus_list = client.list_alloc('test:arcus_list_cache', ArcusTranscoder::FLAG_STRING, 3, cache_time=10)
assert arcus_list == []

items = ['item 1', 'item 2', 'item 3', 'item 4', 'item 5', 'item 6']

for item in items
	arcus_list.push(item)
end

print (arcus_list)
assert arcus_list == items
print (arcus_list[2..4])
assert arcus_list[2..4] == items[2..4]
print (arcus_list[0..2])
assert arcus_list[0..2] == items[0..2]
print (arcus_list[3..-1])
assert arcus_list[3..-1] == items[3..-1]


print('## for loop test')
idx = 0
for a in arcus_list
	puts(a)
	assert a == items[idx]
	idx += 1
end

#####################################################################################################
#
# TEST 7: dynamic set
#
#####################################################################################################


arcus_set = client.set_alloc('test:arcus_set', ArcusTranscoder::FLAG_STRING, 3)
print (arcus_set)

items = ['item 1', 'item 2', 'item 3', 'item 4', 'item 5', 'item 6']

for item in items
	arcus_set.add(item)
end

print (arcus_set)

print('## for loop test')
for a in arcus_set
	print(a)
end

for a in items
	#assert a in arcus_set
end

print ("\n### test done ###\n\n\n")

client.disconnect()







