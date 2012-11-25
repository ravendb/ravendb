/*global $, alert, document, unescape, window*/
/*jslint sloppy:true*/
define(['jquery', 'views/ProfilerButton', 'views/ProfilerView', 'models/ProfilerData'], function ($, ProfilerButton, ProfilerView, ProfilerData) {
    var options,
        profilerData;

    function addResult(session) {
        profilerData.sessions.add(session);
    }

    function fetchResults(id) {
        $.get(options.url, { id: id }, function (obj) {
            if (obj) {
                addResult(obj);
            }
        }, 'json');
    }

    function load() {
        if (options.id.length === 0) {
            return;
        }

        fetchResults(options.id);
    }

    return {
        initialize: function (opt) {
            options = $.extend({}, opt, {});
            profilerData = new ProfilerData();
            var profilerButton = new ProfilerButton({ model: profilerData }),
                profilerView = new ProfilerView({ model: profilerData });
            $('body').append(profilerView.render().el);
            $('body').append(profilerButton.render().el);
            $('head').append($('<link>').attr('rel', 'stylesheet').attr('href', options.url + '?path=styles.css'));

            $('body').ajaxComplete(function (event, xhrRequest, ajaxOptions) {
                if (ajaxOptions.url.indexOf(options.url) !== -1) {
                    return;
                }
                var id = xhrRequest.getResponseHeader('X-RavenDb-Profiling-Id');
                if (id) {
                    fetchResults(id.split(', '));
                }
            });
            load();
        }
    };
});