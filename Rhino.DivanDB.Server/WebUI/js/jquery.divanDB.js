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
                    successCallback(data.docCount);
                }
            });
        },

        getDocumentPage: function(pageNum, pageSize, successCallback) {
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

        getIndexCount: function (successCallback) {
            $.ajax({
                url: settings.server + 'stats',
                success: function (data) {
                    successCallback(data.indexCount);
                }
            });
        },

        getIndexPage: function(pageNum, pageSize, successCallback) {
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
        }
    };
})(jQuery);