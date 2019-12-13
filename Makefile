nginx:
	curl -OL 'http://nginx.org/download/nginx-1.17.3.tar.gz' && \
	  tar -xzvf nginx-1.17.3.tar.gz && rm nginx-1.17.3.tar.gz

pcre:
	curl -OL https://ftp.pcre.org/pub/pcre/pcre-8.41.tar.gz && \
	  tar -xvzf pcre-8.41.tar.gz && rm pcre-8.41.tar.gz

ssl:
	curl -OL https://www.openssl.org/source/openssl-1.1.0g.tar.gz && \
	  tar xvzf openssl-1.1.0g.tar.gz && rm openssl-1.1.0g.tar.gz

conf:
	cd nginx-1.17.3/ && \
	  ./configure --add-module=/Users/wwsun/Sunzhongmou/github/nginx-event-broker \
	  --with-pcre=../pcre-8.41

conf-ssl:
	cd nginx-1.17.3/ && \
	  ./configure --add-module=/Users/wwsun/Sunzhongmou/github/nginx-event-broker \
	  --with-pcre=../pcre-8.41 \
	  --with-http_ssl_module --with-openssl=../openssl-1.1.0g

conf-dy:
	cd nginx-1.17.3/ && \
	  ./configure --add-dynamic-module=/Users/wwsun/Sunzhongmou/github/nginx-event-broker \
	  --with-pcre=../pcre-8.41

build:
	cd nginx-1.17.3/ &&\
	  sudo make -j2 &&\
	  sudo make install

install-test:
	sudo cpan Test::Nginx

test:
	export PATH="/usr/local/nginx/sbin:$PATH" && \
	sudo prove t
