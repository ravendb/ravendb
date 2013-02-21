define(
	[
		'jquery',
		'backbone',
		'underscore',
		'./SessionView',
		'text!./templates/profiler.html',
		'./templateHelper',
		'./RequestDetailsView'
	],
	function ($, Backbone, _, SessionView, profilerTemplate, templateHelper, RequestDetailsView) {
		return Backbone.View.extend({
			className: 'ravendb-profiler-results',
			template: _.template(profilerTemplate),
			totalsTemplate: _.template('{{ helper.round(model.totalRequestDuration()) }} ms waiting for server in {{ model.requestCount() }} request(s) for {{ model.sessionCount() }} sessions(s)'),
			events: {
				'click': 'hideDetailsView',
				'click a': 'close'
			},

			initialize: function () {
				this.model.sessions.on('add', this.renderTotals, this);
				this.model.sessions.on('add', this.addSession, this);
				this.model.on('change:profilerVisible', this.renderVisibility, this);
				$('body').on('keyup', _.bind(this.buttonClick, this));
			},

			render: function () {
				this.renderVisibility();
				this.$el.html(this.template());
				this.$el.append(new RequestDetailsView({ model: this.model }).render().el);
				this.renderTotals();
				return this;
			},

			renderTotals: function () {
				this.$('h1').html(this.totalsTemplate({ model: this.model, helper: templateHelper }));
			},

			addSession: function (session) {
				var sessionView = new SessionView({ model: session });
				this.$('#ravendb-session-container').append(sessionView.render().el);
				if (this.model.get('profilerVisible')) {
				    this.adjustColumns();
				}
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
			    var isVisible = this.model.get('profilerVisible');
			    this.$el.toggle(isVisible);
				if (!isVisible) {
				    this.model.set({ activeRequest: null });
				} else {
				    this.adjustColumns();
				}
			},

			close: function () {
			    this.model.set({ profilerVisible: false });
				return false;
			},

			buttonClick: function (event) {
				if (event.keyCode === 27) { // esc
					if (this.model.get('activeRequest')) {
						this.model.set('activeRequest', null);
					} else {
					    this.model.set({ profilerVisible: false });
					}
				}
			},

			hideDetailsView: function () {
				this.model.set('activeRequest', null);
				return false;
			}

		});
	}
);