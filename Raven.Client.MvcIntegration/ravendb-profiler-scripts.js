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
        $.get(options.url, { id: options.id.join(',') }, function (obj) {
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
                $('<div>' + request.Status + ' ' + request.HttpResult + ' ' + request.Method + ' <span style="overflow:hidden; white-space:nowrap">' + unescape(unescape(request.Url)) + '</span> ' + '</div>')
							.appendTo(resultContainer);
            });
        });
    };

    var createUI = function () {
        $('<style>.rdbprofilerbutton { position:absolute; left: 0; top: 0; background: Orange; border: 1px solid black; cursor: pointer; border-radius: 2px; padding: 0.1em; } .ravendb-profiler-results { display: none; position:absolute; left: 0; top: 1em; border: 1px solid black; background: white; padding: 2em; border-radius: 5px; }</style>')
		    .appendTo('body');

        popupButton = $('<span class="rdbprofilerbutton">RavenDB Profiler</span>')
						.appendTo('body')
						.click(function () {
						    container.toggle();
						});
    };

    return {
        initalize: function (opt) {
            options = opt || { url: '/ravendb/profiling' };
            container = $('<div class="ravendb-profiler-results"></div>')
							.appendTo('body');
            load();
        }
    }
})(jQuery);