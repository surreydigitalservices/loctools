

init:
	bin/setup.sh

clean:
	rm -r cache

HEADERS_URL='https://www.ordnancesurvey.co.uk/docs/product-schemas/addressbase-premium-header-files.zip'
etc/headers:
	curl -s $(HEADERS_URL) > cache/headers.zip
	mkdir -p etc/headers
	unzip -d etc/headers -o cache/headers.zip
