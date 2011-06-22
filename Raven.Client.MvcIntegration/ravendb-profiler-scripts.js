if (!window.jQuery) {
    alert('Please add a reference to jQuery to use RavenDB profiling');
}
else if (!window.jQuery.template) {
    alert('Please add a reference to jQuery-tmpl to use RavenDB profiling');
}

var RavenDBProfiler = (function ($) {
    var options,
        container,
        popupButton,
        resultDialog;

    var templates = ['ravendb-profiler', 'session-template'];

    var load = function () {
        if (options.id.length == 0)
            return;

        templates.forEach(function (name) {
            $.get(options.url, { path: "Templates/" + name + ".tmpl.html" }, function (template) {
                $.template(name, template);
            });
        });

        $.get(options.url, { id: options.id }, function (obj) {
            if (obj)
                addResult(obj);
        }, 'json');
    };

    var fixupTableColumnsWidth = function () {
        var firstRow = $('.session-table thead tr').first();
        var cols = $('th', firstRow);
        var maxs = [0, 0, 0, 0, 0, 0, 0];

        $('.session-table tr').each(function (i, row) {
            for (var i = 0; i < cols.length; i++) {
                maxs[i] = Math.max($($('td', row)[i]).width(), maxs[i]);
            }
        });

        $('.session-table tr').each(function (i, row) {
            for (var i = 0; i < cols.length; i++) {
                $($('td', row)[i]).css('min-width', maxs[i]);
            }
        });
    };

    var addResult = function (resultList) {
        if (!popupButton)
            createUI();

        $.tmpl('ravendb-profiler', resultList).appendTo("#ravendb-session-container");
    };


    var createUI = function () {
        $.get(options.url, { path: 'styles.css' }, function(result)  {
            $('<style>' + result + '</style>').appendTo('body');
        });
           

        popupButton = $('<span class="rdbprofilerbutton">RavenDB Profiler</span>')
            .appendTo('body')
            .click(function () {
                container.toggle();
            });

        $("#ravendb-session-container")
            .delegate('.copy-full-url', 'click', function () {
                var item = $.tmplItem(this);
                alert(item.data.Url);
            })
            .delegate(".toggle-request", "click", function () {

                if (this.collapse) {
                    $('.session-information', $(this).parent()).remove()
                    this.collapse = false;
                    fixupTableColumnsWidth();
                    return;
                }

                this.collapse = true;
                var item = $.tmplItem(this);

                $.tmpl('session-template', item.data, {
                    url: function (str) {
                        return str.split('?')[0];
                    },
                    query: function (str) {
                        var results = str.split('?');
                        if (results.length > 1) {
                            var queryItems = results[1].split('&');
                            return unescape(unescape(queryItems.join('\r\n')));
                        }
                        return "";
                    }
                }).appendTo($(this).parent());

                fixupTableColumnsWidth();
            });
    };

    return {
        initalize: function (opt) {
            options = $.extend({}, opt, { url: '/ravendb/profiling', textRows: 15, textCols: 80 });
            container = $('<div class="ravendb-profiler-results"><h2>Sessions</h2><ol id="ravendb-session-container"></ol></div>')
                .appendTo('body');
            load();
        }
    }
})(jQuery);