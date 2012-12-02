define(
	[
		'jquery',
		'underscore',
		'backbone'
	],
	function ($, _, Backbone) {
		return Backbone.View.extend({
			tagName: 'span',
			className: 'rdbprofilerbutton',
			events: {
				'click': 'toggleProfiler'
			},

			initialize: function () {
				$('body').on('keyup', _.bind(this.buttonClick, this));
			},

			render: function () {
				this.$el.text('RavenDB Profiler');
				return this;
			},

			buttonClick: function (event) {
				if (event.keyCode === 27) { // esc
					this.model.set({ profilerVisibility: false });
				}
			},

			toggleProfiler: function () {
				var currentVisibility = this.model.get('profilerVisibility');
				this.model.set({ profilerVisibility: !currentVisibility });
			}
		});
	}
);