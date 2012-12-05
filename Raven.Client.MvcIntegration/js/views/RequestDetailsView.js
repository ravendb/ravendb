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
				'click': function () { return false; },
				'click a': 'close'
			},

			initialize: function () {
				this.model.on('change:activeRequest', this.render, this);
			},

			render: function () {
				var activeRequest = this.model.get('activeRequest');
				if (!activeRequest) {
					this.$el.empty();
					this.$el.hide();
					return this;
				}

				this.$el.html(this.template({ data: activeRequest.toJSON(), helper: templateHelper }));
				this.$el.show();
				this.$('.query').toggle(templateHelper.query(activeRequest.get('Url')).length > 0);
				this.$('.postData').toggle(activeRequest.has('PostedData') && activeRequest.get('PostedData').length > 0);
				return this;
			},

			close: function () {
				this.model.set('activeRequest', null);
				return false;
			}
		});
	}
);