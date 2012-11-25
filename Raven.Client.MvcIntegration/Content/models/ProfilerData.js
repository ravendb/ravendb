define(['backbone', './Session'], function (Backbone, Session) {
    return Backbone.Model.extend({
        defaults: {
            profilerVisibility: false
        },

        initialize: function () {
            this.sessions = new Backbone.Collection(null, { model: Session });
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
        }
    });
});