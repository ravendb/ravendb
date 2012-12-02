/*global window*/
define(
	[
		'backbone',
		'underscore',
		'./templateHelper',
		'text!./templates/request.html',
		'./RequestDetailsView'
	],
	function (Backbone, _, templateHelper, requestTemplate, RequestDetailsView) {
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
				var view = new RequestDetailsView({ model: this.model });
				this.$el.append(view.render().el);
			}
		});
	}
);