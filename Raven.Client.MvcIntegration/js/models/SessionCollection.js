define(['backbone', './Session'], function (Backbone, Session) {
    return Backbone.Collection.extend({
        model: Session,
        initialize: function (models, options) {
            this.url = options.url;
        }
    });
});