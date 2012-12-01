define(
    [
        'jquery',
        'backbone',
        'underscore',
        './SessionView',
        'text!./templates/profiler.html',
        './templateHelper'
    ],
    function ($, Backbone, _, SessionView, template, templateHelper) {
        return Backbone.View.extend({
            className: 'ravendb-profiler-results',
            template: _.template(template),
            totalsTemplate: _.template('<%= helper.round(model.totalRequestDuration()) %> ms waiting for server in <%= model.requestCount() %> request(s) for <%= model.sessionCount() %> sessions(s)'),
            events: {
                'click a.close': 'close'

            },
            initialize: function () {
                this.model.sessions.on('change', this.renderTotals, this);
                this.model.sessions.on('add', this.addSession, this);
                this.model.on('change:profilerVisibility', this.renderVisibility, this);
            },

            render: function () {
                this.$el.hide();
                this.$el.html(this.template());
                this.renderTotals();
                this.renderSessions();
                return this;
            },

            renderTotals: function () {
                this.$('h1').html(this.totalsTemplate({ model: this.model, helper: templateHelper }));
            },

            renderSessions: function () {
                var sessionViews = [];
                this.model.sessions.each(function (session) {
                    var sessionView = new SessionView({ model: session });
                    sessionViews.push(sessionView.render().el);
                });
                this.$('#ravendb-session-container').append(sessionViews);
            },

            addSession: function (session) {
                var sessionView = new SessionView({ model: session });
                this.$('#ravendb-session-container').append(sessionView.el);
                this.adjustColumns();
            },

            adjustColumns: function () {
                var maxs = [],
                    tableRows = this.$('.session-table > tbody > tr');

                tableRows.each(function () {
                    $(this).children().each(function (i, cell) {
                        maxs[i] = Math.max($(cell).width(), maxs[i] || 0);
                    });
                });

                tableRows.each(function () {
                    $(this).children().each(function (i, cell) {
                        $(cell).css('min-width', maxs[i]);
                    });
                });
            },

            renderVisibility: function () {
                this.$el.toggle(this.model.get('profilerVisibility'));
            },

            close: function () {
                var currentVisibility = this.model.get('profilerVisibility');
                this.model.set({ profilerVisibility: !currentVisibility });
                return false;
            }
        });
    }
);