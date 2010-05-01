var indexSearchCache = null;

(function ($) {
    $.ravenDB = {
        settings: null,

        init: function (options) {
            settings = $.extend({
                server: '/'
            }, options);
        },

        getStatistics: function (successCallback) {
            $.ajax({
                url: settings.server + 'stats',
                dataType: 'json',
                success: function (data) {
                    successCallback(data);
                }
            });
        },

        getDocumentCount: function (successCallback) {
            $.ajax({
                url: settings.server + 'stats',
                dataType: 'json',
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
                data: {
                    start: start,
                    pageSize: pageSize
                },
                success: function (data) {
                    successCallback(data);
                }
            });
        },

        getDocument: function (id, operation, successCallback) {
            $.ajax({
                url: settings.server + 'docs/' + id,
                dataType: 'json',
                complete: function(xhr) {
                    switch(xhr.status) 
                    {
                        case 200:
                            var data = JSON.parse(xhr.responseText);
                            var etag = xhr.getResponseHeader("Etag");
                            var template = xhr.getResponseHeader('Raven-' + operation + '-Template');
                            successCallback(data, etag, template);
                            break;
                        case 404:
                            successCallback(null, null, null);
                            break;
                    }
                }
            });
        },

        saveDocument: function (id, etag, template, json, successCallback, errorCallback) {
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
                beforeSend: function(xhr) {
                    if (etag)
                        xhr.setRequestHeader("If-Match", etag); 
                    if (template)
                        xhr.setRequestHeader('Raven-View-Template', template);       
                },
                success: function (data) {
                    successCallback(data);
                },
                error: function(data){
                    var m = JSON.parse(data.responseText);
                    if(errorCallback != undefined){
                        errorCallback(m.error);
                    }
                }
            });
        },
        
        deleteDocument: function(id, etag, successCallback) {
            $.ajax({
                type: 'DELETE',
                url: settings.server + 'docs/' + id,
                beforeSend: function(xhr) {
                    xhr.setRequestHeader("If-Match", etag);        
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
                data: {definition: 'yes'},
                success: function (data) {
                    successCallback(data);
                }
            });
        },

        saveIndex: function (name, mapDef, reduceDef, successCallback, errorCallback) {
            var data = JSON.stringify({Map: mapDef});
            if(reduceDef)
                data += "," + JSON.stringify({Reduce: reduceDef});
            $.ajax({
                type: 'PUT',
                url: settings.server + 'indexes/' + name,
                data: JSON.stringify({Map: mapDef}),
                success: function (data) {
                    successCallback(data);
                },
                error: function(request, textStatus, errorThrown) {
                    errorCallback();
                }
            });
        },
        
        deleteIndex: function(name, successCallback) {
            $.ajax({
                type: 'DELETE',
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

        searchIndexes: function (name, successCallback) {
            name = name.toLowerCase();

            if (indexSearchCache == null) {
                //this should do server side searching, but that's not implemented yet
                this.getIndexPage(1, 1000, function (data) {
                    indexSearchCache = data;
                    var indexes = new Array();
                    $(data).each(function () {
                        if (this.name.toLowerCase().indexOf(name) > 0) {
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