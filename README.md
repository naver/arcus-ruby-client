
## arcus-ruby-client : Arcus Ruby Client

This is a ruby client driver for Arcus cloud.

## Requirement

This driver is made under ruby 2.1.1.
And it needs bisect, zookeeper module. Install them by gem first.

This module uses epoll system call but ruby does not support that.
So, epoll C extension should be built like below.

```
cd ./epoll_ext
make
sudo make install
```


## Use

Just import arcus.rb and arcus_mc_node.rb
test.rb is basic functional test for this driver and you can get detail information about that.

Visit arcus cache cloud project at github to get more detail information.
https://github.com/naver/arcus


## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0



