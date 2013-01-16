var indexSearchCache = null;

(function ($) {
    $.ravenDB = {
        settings: null,

        headersToIgnore: [
          // Entity headers - those are NOT ignored
          /*
                "content-disposition",
                "content-encoding",
                "content-language",
                "content-location",
                "content-md5",
                "expires",
          */
          "content-type", // always the same for documents, no point in accepting it
          "origin",
          "allow",
          "content-range",
          "last-modified",
          // Ignoring this header, since it may
          // very well change due to things like encoding,
          // adding metadata, etc
          "content-length",
		  "content-encoding",
          // Special things to ignore
          "keep-alive",
          "x-requested-with",
          // Request headers
          "accept-charset",
          "accept-encoding",
          "accept",
          "accept-language",
          "authorization",
          "cookie",
		  "x-aspnet-version",
		  "x-powered-by",
		  "x-sourcefiles",
          "expect",
          "from",
          "host",
          "if-match",
          "if-modified-since",
          "if-none-match",
          "if-range",
          "if-unmodified-since",
          "max-forwards",
          "referer",
          "te",
          "user-agent",
          //Response headers
          "accept-ranges",
          "age",
          "allow",
          "etag",
          "location",
          "retry-after",
          "server",
          "set-cookie2",
          "set-cookie",
          "vary",
          "www-authenticate",
          // General
          "cache-control",
          "connection",
          "date",
          "pragma",
          "trailer",
          "transfer-encoding",
          "upgrade",
          "via",
          "warning",
        ],
		
		getServerUrl: function() {
			return settings.server;
		},

        init: function (options) {

			var scriptSource = (function() {
				var scripts = document.getElementsByTagName('script');
				var scriptSrc = null;
				for (var i = 0; i < scripts.length; i++) {
					if (scripts[i].src != null) {
						if(scripts[i].getAttribute('src', -1) != null && 
							scripts[i].getAttribute('src', -1).length > scripts[i].length){
							scriptSrc = scripts[i].getAttribute('src', -1);	
						}
						else {
							scriptSrc = scripts[i].src;
						}
					}
					else {
						scriptSrc = scripts[i].getAttribute('src', -1);
					}
					var indexPosition =  scriptSrc.indexOf('jquery.RavenDB.js');
					if(indexPosition != -1)
						break;
				}
				var indexPosition =  scriptSrc.indexOf('raven/js/jquery.RavenDB.js');
				return scriptSrc.substring(0, indexPosition);
			}());

            settings = $.extend({
                server: scriptSource
            }, options);
        },

        getStatistics: function (successCallback) {
            $.ajax({
                url: settings.server + 'stats',
                dataType: 'json',
                cache: false,
                success: function (data) {
                    successCallback(data);
                }
            });
        },

        getDocumentCount: function (successCallback) {
            $.ajax({
                url: settings.server + 'stats',
                dataType: 'json',
                cache: false,
                success: function (data) {
                    successCallback(data.CountOfDocuments);
                }
            });
        },

        getDocumentPage: function (pageNum, pageSize, successCallback) {
            var start = (pageNum - 1) * pageSize;

            $.ajax({
                url: settings.server + 'docs/',
                dataType: 'json',
                cache: false,
                data: {
                    start: start,
                    pageSize: pageSize
                },
                success: function (data) {
                    successCallback(data);
                }
            });
        },

        splitAndFilterHeaders: function(headersAsSingleString) {
            var headers = {};
            var headersLines = headersAsSingleString.replace(/\r\n/g,"\n").split('\n');
            for (var i = 0; i < headersLines.length; i++) {
                var line = headersLines[i];
                var keyStart = line.indexOf(': ');
                if(keyStart == -1)
                    continue;
                var key = line.substring(0, keyStart);

                if($.inArray(key.toLowerCase(), $.ravenDB.headersToIgnore) != -1)
                    continue;

                headers[key] = line.substring(keyStart+2);
            }
            return headers;
        },

         getDocument: function (id, successCallback) {
            $.ajax({
                url: settings.server + 'docs/' + id,
                dataType: 'json',
                cache: false,
                complete: function(xhr) {
                    switch(xhr.status) 
                    {
                        case 200:
                            var data = JSON.parse(xhr.responseText);
                            var etag = xhr.getResponseHeader("Etag");
                            var headersAsSingleString = xhr.getAllResponseHeaders();
                            var headers = $.ravenDB.splitAndFilterHeaders(headersAsSingleString);
                            successCallback(data, etag, headers);
                            break;
                        case 404:
                            successCallback(null, null, null);
                            break;
                    }
                }
            });
        },

        saveDocument: function (id, etag, metadata, json, successCallback, errorCallback) {
            var idStr = '';
            var type = 'POST';
            if (id != null) {
                idStr = id;
                type = 'PUT';
            }
            $.ajax({
                type: type,
                url: settings.server + 'docs/' + idStr,
                data: json,
                cache: false,
                beforeSend: function(xhr) {
                    if (etag)
                        xhr.setRequestHeader("If-None-Match", etag); 
                    if (metadata) {
                       for (var key in metadata) {
                            xhr.setRequestHeader(key, metadata[key]);       
                        }
                    }
                },
                success: function (data) {
                    successCallback(data);
                },
                error: function(data){
                    var m = JSON.parse(data.responseText);
                    if(errorCallback != undefined){
                        errorCallback(m.Error);
                    }
                }
            });
        },
        
        deleteDocument: function(id, etag, successCallback) {
            $.ajax({
                type: 'DELETE',
                url: settings.server + 'docs/' + id,
                cache: false,
                beforeSend: function(xhr) {
                    xhr.setRequestHeader("If-None-Match", etag);        
                },
                success: function (data) {
                    successCallback(data);
                }
            });
        },

        getIndexCount: function (successCallback) {
            $.ajax({
                url: settings.server + 'stats',
                dataType: 'json',
                cache: false,
                success: function (data) {
                    successCallback(data.CountOfIndexes);
                }
            });
        },

        getIndexPage: function (pageNum, pageSize, successCallback) {
            var start = (pageNum - 1) * pageSize;

            $.ajax({
                url: settings.server + 'indexes/',
                dataType: 'json',
                cache: false,
                data: {
                    start: start,
                    pageSize: pageSize
                },
                success: function (data) {
                    successCallback(data);
                }
            });
        },

        getIndex: function (name, successCallback) {
            $.ajax({
                url: settings.server + 'indexes/' + name,
                dataType: 'json',
                cache: false,
                data: {definition: 'yes'},
                success: function (data) {
                    successCallback(data);
                }
            });
        },

        saveIndex: function (name, indexDef, successCallback, errorCallback) {
            $.ajax({
                type: 'PUT',
                url: settings.server + 'indexes/' + name,
                data: JSON.stringify(indexDef),
                cache: false,
                success: function (data) {
                    successCallback(data);
                },
                error: function(request, textStatus, errorThrown) {
					var result = JSON.parse(request.responseText)
                    errorCallback(result);
                }
            });
        },
        
        deleteIndex: function(name, successCallback) {
            $.ajax({
                type: 'DELETE',
                cache: false,
                url: settings.server + 'indexes/' + name,
                success: function (data) {
                    successCallback(data);
                }
            });
        },

        queryIndex: function (name, queryValues, pageNumber, pageSize, successCallback) {
            var start = (pageNumber - 1) * pageSize;

            $.ajax({
                type: 'GET',
                url: settings.server + 'indexes/' + name,
                dataType: 'json',
                cache: false,
                data: { 
                    query : queryValues,
                    start: start,
                    pageSize: pageSize
                },
                success: function (data) {
                    successCallback(data);
                }
            });
        },

         queryLinqIndex: function (linqQuery, pageNumber, pageSize, successCallback) {
            var start = (pageNumber - 1) * pageSize;
            $.ajax({
                type: 'POST',
                url: settings.server + 'linearQuery',
                cache: false,
                contentType: 'application/json; charset=utf-8',
                data: JSON.stringify({ 
                    Query : linqQuery,
                    Start: start,
                    PageSize: pageSize
                }),
                success: function (data) {
                    successCallback(data);
                }
            });
        },

        searchIndexes: function (name, successCallback) {
            name = name.toLowerCase();

            if (indexSearchCache == null) {
                //this should do server side searching, but that's not implemented yet
                this.getIndexPage(1, 1000, function (data) {
                    indexSearchCache = data;
                    var indexes = new Array();
                    $(data).each(function () {
                        if (this.name.toLowerCase().indexOf(name) > -1) {
                            indexes.push(this.name);
                        }
                    });

                    successCallback(indexes);
                });
            } else {
                var indexes = new Array();

                $(indexSearchCache).each(function () {
                    if (this.name.toLowerCase().indexOf(name) >= 0) {
                        indexes.push(this.name);
                    }
                });

                successCallback(indexes);
            }
        }
    };
})(jQuery);

(function(){
	var IE4 = (navigator.appVersion.indexOf("MSIE 4.")==-1)? false : true;
	var IE5 = (navigator.appVersion.indexOf("MSIE 5.")==-1) ? false : true;
	var IE6 = (navigator.appVersion.indexOf("MSIE 6.")==-1) ? false : true;
	var IE7 = (navigator.appVersion.indexOf("MSIE 7.")==-1) ? false : true;

	if(IE4 || IE5 || IE6 || IE7)
	{
		alert("Your browser is unsupported, please use IE 8 or higher, Chrome or FireFox\n" +
			  "You can still try to use the administrator UI, but things may not work");
	}
})(jQuery);
