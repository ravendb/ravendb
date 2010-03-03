var indexSearchCache = null;

(function ($) {
    $.divanDB = {
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

        getDocument: function (id, successCallback) {
            $.ajax({
                url: settings.server + 'docs/' + id,
                dataType: 'json',
                complete: function(xhr) {
                    if (xhr.status == 200) {
                        var data = JSON.parse(xhr.responseText);
                        var etag = xhr.getResponseHeader("Etag");
                        successCallback(data, etag);
                    }
                }
            });
        },

        saveDocument: function (id, etag, json, successCallback) {
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

        saveIndex: function (name, def, successCallback) {
            $.ajax({
                type: 'PUT',
                url: settings.server + 'indexes/' + name,
                data: def,
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