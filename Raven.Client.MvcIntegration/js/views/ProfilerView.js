define(
	[
		'jquery',
		'backbone',
		'underscore',
		'./SessionView',
		'text!./templates/profiler.html',
		'./templateHelper'
	],
	function ($, Backbone, _, SessionView, profilerTemplate, templateHelper) {
		return Backbone.View.extend({
			className: 'ravendb-profiler-results',
			template: _.template(profilerTemplate),
			totalsTemplate: _.template('{{ helper.round(model.totalRequestDuration()) }} ms waiting for server in {{ model.requestCount() }} request(s) for {{ model.sessionCount() }} sessions(s)'),
			events: {
				'click a.close': 'close'
			},

			initialize: function () {
				this.model.sessions.on('add', this.renderTotals, this);
				this.model.sessions.on('add', this.addSession, this);
				this.model.on('change:profilerVisibility', this.renderVisibility, this);
			},

			render: function () {
				this.renderVisibility();
				this.$el.html(this.template());
				this.renderTotals();
				return this;
			},

			renderTotals: function () {
				this.$('h1').html(this.totalsTemplate({ model: this.model, helper: templateHelper }));
			},

			addSession: function (session) {
				var sessionView = new SessionView({ model: session });
				this.$('#ravendb-session-container').append(sessionView.render().el);
				this.adjustColumns();
			},

			adjustColumns: function () {
				var maxs = [],
					tableRows = this.$('.session-table > tbody > tr');

				tableRows.each(function () {
					$(this).children().each(function (i, cell) {
						maxs[i] = Math.max($(cell).width(), maxs[i] || 0);
					});
				});

				tableRows.each(function () {
					$(this).children().each(function (i, cell) {
						$(cell).css('min-width', maxs[i]);
					});
				});
			},

			renderVisibility: function () {
				var visibility = this.model.get('profilerVisibility') ? 'visible' : 'hidden';
				this.$el.css({ visibility: visibility });
			},

			close: function () {
				var currentVisibility = this.model.get('profilerVisibility');
				this.model.set({ profilerVisibility: !currentVisibility });
				return false;
			}
		});
	}
);