define(['backbone', 'underscore', 'text!./templates/requestDetails.html!strip', './templateHelper'], function (Backbone, _, requestDetailsTemplate, templateHelper) {
    return Backbone.View.extend({
        template: _.template(requestDetailsTemplate),
        className: 'request-details',
        events: {
            'click a.close': 'close'
        },
        render: function () {
            this.$el.html(this.template({ data: this.model.toJSON(), helper: templateHelper }));
            return this;
        },

        close: function () {
            this.remove();
            return false;
        }
    });
});