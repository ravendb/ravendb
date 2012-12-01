/*global $, alert, document, unescape, window*/
define(['jquery', 'views/ProfilerButton', 'views/ProfilerView', 'models/ProfilerData'], function ($, ProfilerButton, ProfilerView, ProfilerData) {
    return function (options) {
        var profilerData = new ProfilerData({ sessionUrl: options.rootUrl + "?id=" });
        profilerData.addSessions(options.sessionIds);

        $('head').append($('<link>').attr('rel', 'stylesheet').attr('href', options.rootUrl + '?path=styles.css'));
        $('body')
            .append(new ProfilerView({ model: profilerData }).render().el)
            .append(new ProfilerButton({ model: profilerData }).render().el)
            .on('ajaxComplete', _.bind(profilerData.handleResponse, profilerData));
    };
});