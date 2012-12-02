define(
    [
        'underscore',
        'backbone',
        './SessionCollection'
    ],
    function (_, Backbone, SessionCollection) {
        return Backbone.Model.extend({
            defaults: {
                profilerVisibility: false
            },

            initialize: function (options) {
                this.sessions = new SessionCollection(null, { url: options.sessionUrl });
            },

            addSessions: function (sessionIdList) {
                _(sessionIdList).each(function (id) {
                    this.sessions.add({ id: id });
                }, this);
            },

            totalRequestDuration: function () {
                return this.sessions.reduce(function (total, session) {
                    return total + session.totalRequestDuration();
                }, 0);
            },

            requestCount: function () {
                return this.sessions.reduce(function (total, session) {
                    return total + session.requestCount();
                }, 0);
            },

            sessionCount: function () {
                return this.sessions.length;
            },

            handleResponse: function (event, xhrRequest) {
                var headerIds = xhrRequest.getResponseHeader('X-RavenDb-Profiling-Id');
                this.addSessions(headerIds.split(', '));
            }
        });
    }
);