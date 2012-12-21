define(
	[
		'backbone',
		'underscore',
		'./templateHelper',
		'text!./templates/request.html'
	],
	function (Backbone, _, templateHelper, requestTemplate) {
		return Backbone.View.extend({
			template: _.template(requestTemplate),
			tagName: 'tr',
			events: {
				'click .show-full-url': 'showFullUrl',
				'click .show-request-details': 'showDetails'
			},

			render: function () {
				this.$el.html(this.template({ data: this.model.toJSON(), helper: templateHelper }));
				return this;
			},

			showFullUrl: function () {
				window.alert(this.model.get('Url'));
				return false;
			},

			showDetails: function () {
				this.model.showDetails();
				return false;
			}
		});
	}
);