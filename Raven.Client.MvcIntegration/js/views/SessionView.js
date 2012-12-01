define(['backbone', 'underscore', './RequestView', 'text!./templates/session.html'], function (Backbone, _, RequestView, sessionTemplate) {
    return Backbone.View.extend({
        template: _.template(sessionTemplate),

        initialize: function () {
            this.model.on('change', this.render, this);
        },

        render: function () {
            var requestViews = [];
            this.$el.html(this.template({ sessionDuration: this.model.sessionDuration(), requestCount: this.model.requests.length }));
            this.model.requests.each(function (request) {
                var requestView = new RequestView({ model: request });
                requestViews.push(requestView.render().el);
            });
            this.$('tbody').append(requestViews);
            return this;
        }
    });
});