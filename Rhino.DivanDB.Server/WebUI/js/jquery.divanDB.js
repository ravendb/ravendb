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
                success: function (data) {
                    successCallback(data);
                }
            });
        },

        getDocumentCount: function (successCallback) {
            $.ajax({
                url: settings.server + 'stats',
                success: function (data) {
                    successCallback(data.CountOfDocuments);
                }
            });
        },

        getDocumentPage: function (pageNum, pageSize, successCallback) {
            var start = (pageNum - 1) * pageSize;

            $.ajax({
                url: settings.server + 'docs/',
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
                success: function (data) {
                    successCallback(data);
                }
            });
        },

        saveDocument: function (id, json, successCallback) {
            var idStr = '';
            if (id != null)
                idStr = id;
            $.ajax({
                type: 'PUT',
                url: settings.server + 'docs/' + idStr,
                data: json,
                success: function (data) {
                    successCallback(data);
                }
            });
        },

        getIndexCount: function (successCallback) {
            $.ajax({
                url: settings.server + 'stats',
                success: function (data) {
                    successCallback(data.indexCount);
                }
            });
        },

        getIndexPage: function (pageNum, pageSize, successCallback) {
            var start = (pageNum - 1) * pageSize;

            $.ajax({
                url: settings.server + 'indexes/',
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