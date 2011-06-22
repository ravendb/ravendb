if (!window.jQuery) {
    alert('Please add a reference to jQuery to use RavenDB profiling');
}

var RavenDBProfiler = (function ($) {
    var options,
        container,
        popupButton,
        resultDialog;

    var load = function () {
        if (options.id.length == 0)
            return;
        fetchResults(options.id.join(','));
    };

    var fetchResults = function (idList) {
        $.get(options.url, { id: idList }, function (obj) {
            if (obj)
                addResult(obj);
        }, 'json');
    };

    var addResult = function (resultList) {
        if (!popupButton)
            createUI();

        resultList.forEach(function (result) {
            var resultContainer = $('<div class="resultContainer"><span>Id: ' + result.Id + '</span></div>')
							.appendTo(container)
            result.Requests.forEach(function (request) {
                addRequest(request, resultContainer);
            });
        });
    };

    var addRequest = function (request, resultContainer) {
        var requestHolder = $('<div>' + request.Status + ' ' + request.HttpResult + ' ' + request.Method + ' <span style="overflow:hidden; white-space:nowrap">' + unescape(unescape(request.Url)) + '</span> ' + '</div>')
                .appendTo(resultContainer);
        if (request.Result) {
            addResultToggle(requestHolder);
            addRequestResultText(request.Result, requestHolder);
        }
    };

    var addResultToggle = function (requestHolder) {
        $('<a href="#">+</a> ').click(function () {
            $(this).parent().find('.rdbResultHolder').toggle();
            $(this).html($(this).html() == '-' ? '+' : '-');
            return false;
        }).prependTo(requestHolder);
    };

    var addRequestResultText = function (text, requestHolder) {
        var resultHolder = $('<div class="rdbResultHolder" style="display:none;"></div>').html('<textarea rows=' + options.textRows + ' cols=' + options.textCols + '>' + text + '</textarea>').appendTo(requestHolder);
    };

    var createUI = function () {
        $('<style>.rdbprofilerbutton { position:absolute; left: 0; top: 0; background: Orange; border: 1px solid black; cursor: pointer; border-radius: 2px; padding: 0.1em; } .ravendb-profiler-results { display: none; position:absolute; left: 0; top: 1em; border: 1px solid black; background: white; padding: 2em; border-radius: 5px; } .rdbResultHolder { padding-left: 1em; }</style>')
            .appendTo('body');

        popupButton = $('<span class="rdbprofilerbutton">RavenDB Profiler</span>')
            .appendTo('body')
            .click(function () {
                container.toggle();
            });
    };

    var listenForAjax = function () {
        $('body').ajaxComplete(function (event, xhrRequest, ajaxOptions) {
            var id = xhrRequest.getResponseHeader('X-RavenDb-Profiling-Id');
            if (id)
                fetchResults(id);
        });
    };

    return {
        initalize: function (opt) {
            options = $.extend({}, opt, { url: '/ravendb/profiling', textRows: 15, textCols: 80 });
            container = $('<div class="ravendb-profiler-results"></div>')
                .appendTo('body');
            load();
            listenForAjax();
        }
    }
})(jQuery);