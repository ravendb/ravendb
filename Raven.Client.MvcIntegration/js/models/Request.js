define(
	[
		'backbone'
	],
	function (Backbone) {
		return Backbone.Model.extend({
			defaults: {
				PostedData: 'none'
			},

			showDetails: function () {
				this.trigger('toggleRequestDetails', this);
			}
		});
	}
);