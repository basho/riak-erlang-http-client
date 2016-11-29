# riak-erlang-http-client

## Build Status

[![Build Status](https://travis-ci.org/basho/riak-erlang-http-client.svg?branch=develop)](https://travis-ci.org/basho/riak-erlang-http-client)

`riak-erlang-http-client` is an Erlang client for Riak, using the HTTP interface

## Quick Start

You must have [Erlang/OTP R15B01](http://erlang.org/download.html) or later and a GNU-style build system to compile and run `riak-erlang-http-client`.

```sh
git clone git://github.com/basho/riak-erlang-http-client.git
cd riak-erlang-http-client
make
```

If the Protocol Buffers Riak Erlang Client ([riak-erlang-client](http://github.com/basho/riak-erlang-client)) is already familiar to you, you should find this client familiar. Just substitute calls to `riakc_pb_socket` with calls to `rhc`.

As a quick example, here is how to create and retrieve a value with the key "foo" in the bucket "bar" using this client.

First, start up an Erlang shell with the path to `riak-erlang-http-client` and all dependencies included, then start `sasl` and `ibrowse`:

```sh
erl -pa path/to/riak-erlang-http-client/ebin path/to/riak-erlang-http-client/deps/*/ebin
Eshell V5.8.2  (abort with ^G)
1> [ ok = application:start(A) || A <- [sasl, ibrowse] ].
```

Next, create your client:

```erlang
2> IP = "127.0.0.1",
2> Port = 8098,
2> Prefix = "riak",
2> Options = [],
2> C = rhc:create(IP, Port, Prefix, Options).
{rhc,"10.0.0.42",80,"riak",[{client_id,"ACoc4A=="}]}
```

Sidenote: if you will be using the defaults, as in the example above, you may call `rhc:create/0` instead of specifying the defaults yourself.

Create a new object, and store it with `rhc:put/2`:

```erlang
3> Bucket = <<"bar">>,
3> Key = <<"foo">>,
3> Data = <<"hello world">>,
3> ContentType = <<"text/plain">>,
3> Object0 = riakc_obj:new(Bucket, Key, Data, ContentType),
3> rhc:put(C, Object0).
ok
```

Retrieve an object with `rhc:get/3`:

```erlang
4> Bucket = <<"bar">>,
4> Key = <<"foo">>,
4> {ok, Object1} = rhc:get(C, Bucket, Key).
{ok,{riakc_obj,<<"bar">>,<<"foo">>,
               <<107,206,97,96,96,96,204,96,202,5,82,44,12,167,92,95,100,
                 48,37,50,230,177,50,...>>,
               [{{dict,3,16,16,8,80,48,
                       {[],[],[],[],[],[],[],[],[],[],[],[],...},
                       {{[],[],[],[],[],[],[],[],[],[],...}}},
                 <<"hello world">>}],
               undefined,undefined}}
```

Please refer to the generated documentation for more information:

```sh
make doc && open doc/index.html
```

## Contributing

We encourage contributions to `riak-erlang-http-client` from the community.

* Fork the `riak-erlang-http-client` repository on [GitHub](https://github.com/basho/riak-erlang-http-client)

* Clone your fork or add the remote if you already have a clone of the repository.

        git clone git@github.com:yourusername/riak-erlang-http-client.git
        # or
        git remote add mine git@github.com:yourusername/riak-erlang-http-client.git

* Create a topic branch for your change.

        git checkout -b some-topic-branch

* Make your change and commit. Use a clear and descriptive commit message, spanning multiple lines if detailed explanation is needed.

* Push to your fork of the repository and then send a pull-request through Github.

        git push mine some-topic-branch

* A Basho engineer or community maintainer will review your patch and merge it into the main repository or send you feedback.
