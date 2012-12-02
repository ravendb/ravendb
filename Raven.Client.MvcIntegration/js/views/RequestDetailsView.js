define(
	[
		'backbone',
		'underscore',
		'text!./templates/requestDetails.html!strip',
		'./templateHelper'
	],
	function (Backbone, _, requestDetailsTemplate, templateHelper) {
		return Backbone.View.extend({
			template: _.template(requestDetailsTemplate),
			className: 'request-details',
			events: {
				'click a.close': 'close'
			},

			render: function () {
				this.$el.html(this.template({ data: this.model.toJSON(), helper: templateHelper }));
				this.toggleQueryDisplay();
				return this;
			},

			toggleQueryDisplay: function () {
				var hasQuery = templateHelper.query(this.model.get('Url')).length > 0;
				this.$('.query').toggle(hasQuery);
			},

			close: function () {
				this.remove();
				return false;
			}
		});
	}
);