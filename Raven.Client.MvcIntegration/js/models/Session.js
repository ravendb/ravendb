define(
	[
		'backbone',
		'./Request'
	],
	function (Backbone, Request) {
		return Backbone.Model.extend({
			initialize: function (data) {
				this.requests = new Backbone.Collection(data.Requests, { model: Request });
			},

			parse: function (response) {
				return response[0];
			},

			totalRequestDuration: function () {
				var duration = this.requests.reduce(function (total, request) {
					return total + request.get('DurationMilliseconds');
				}, 0);
				return duration;
			}
		});
	}
);