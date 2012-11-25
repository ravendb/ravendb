define(['backbone', './Request'], function (Backbone, Request) {
    return Backbone.Model.extend({
        initialize: function (data) {
            this.requests = new Backbone.Collection(data.Requests, { model: Request });
        },

        totalRequestDuration: function () {
            var duration = this.requests.reduce(function (total, request) {
                return total + request.get('DurationMilliseconds');
            }, 0);
            return this.round(duration);
        },

        sessionDuration: function () {
            return this.round(this.get('DurationMilliseconds'));
        },

        round: function (n) {
            return Math.round(n * 100) / 100; // round to 2 points after period 
        },

        requestCount: function () {
            return this.requests.length;
        }
    });
});