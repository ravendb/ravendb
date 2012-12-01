define(
    [
        'backbone',
        './Request'
    ],
    function (Backbone, Request) {
        return Backbone.Model.extend({
            initialize: function () {
                this.requests = new Backbone.Collection(null, { model: Request });
                this.on('change', function () {
                    this.requests.reset(this.get('Requests'));
                }, this);
                this.fetch();
            },

            parse: function (response) {
                return response[0];
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
            },

            url: function () {
                return this.collection.url + this.id;
            }
        });
    }
);