define(
	[
		'jquery',
		'underscore',
		'backbone',
		'./Session'
	],
	function ($, _, Backbone, Session) {
		return Backbone.Model.extend({
			defaults: {
				profilerVisibility: false
			},

			initialize: function (options) {
				this.sessionUrl = options.sessionUrl;
				this.sessions = new Backbone.Collection(null, { model: Session });
			},

			loadSessionData: function (sessionIdList) {
				var sessionCollection = this.sessions;
				$.get(this.sessionUrl, { id: sessionIdList }, function (sessions) {
					sessionCollection.add(sessions);
				});
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
					return total + session.requests.length;
				}, 0);
			},

			sessionCount: function () {
				return this.sessions.length;
			},

			handleResponse: function (event, xhrRequest) {
				var headerIds = xhrRequest.getResponseHeader('X-RavenDb-Profiling-Id');
				this.loadSessionData(headerIds.split(', '));
			}
		});
	}
);