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
				profilerVisible: false,
				activeRequest: null
			},

			initialize: function (options) {
				this.sessionUrl = options.sessionUrl;
				this.sessions = new Backbone.Collection(null, { model: Session });
				this.sessions.on('toggleRequestDetails', this.toggleRequestDetails, this);
			},

			loadSessionData: function (sessionIdList) {
				if (sessionIdList.length == 0) {
					return;
				}
				
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
				if (!headerIds) {
					return;
				}
				this.loadSessionData(headerIds.split(', '));
			},

			toggleRequestDetails: function (request) {
				var activeRequest = this.get('activeRequest');
				this.set('activeRequest', (activeRequest !== request) ? request : null);
			}
		});
	}
);
