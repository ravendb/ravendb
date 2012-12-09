define(
	[
		'backbone'
	],
	function (Backbone) {
		return Backbone.View.extend({
			tagName: 'span',
			className: 'rdbprofilerbutton',
			events: {
				'click': 'toggleProfiler'
			},

			render: function () {
				this.$el.text('RavenDB Profiler');
				return this;
			},

			toggleProfiler: function () {
				var currentVisibility = this.model.get('profilerVisibility');
				this.model.set({ profilerVisibility: !currentVisibility });
			}
		});
	}
);