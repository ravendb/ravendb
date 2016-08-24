
import * as EVENTS from "common/constants/events"

/*
    Events emitted through ko.postbox
        * ResourceSwitcher.Show - when searchbox is opened
        * ResourceSwitcher.Hide - when searchbox is hidden
        * ResourceSwitcher.ItemSelected - item selected from resource switcher pane
*/
class resourceSwitcher {

    private $selectDatabaseContainer: JQuery;
    private $selectDatabase: JQuery;
    private $filter: JQuery;

    initialize() {
        this.$selectDatabaseContainer = $('.select-database-container');
        this.$selectDatabase = $('.form-control.btn-toggle.select-database');
        this.$filter = $('.select-database-container .database-filter');

        this.$selectDatabaseContainer.on('click', (e) => {
            e.stopPropagation();
            this.show();
        });

        this.$selectDatabase.on('click', (e) => {
            if (this.$selectDatabase.is('.active')) {
                this.hide();
            } else {
                this.show();
            }

            e.stopPropagation();
        });

        let hide = () => this.hide();

        $('.select-database-container .box-container a').on('click', function (e) {
            e.stopPropagation();
            hide();
            let a: HTMLAnchorElement = this as HTMLAnchorElement;
            ko.postbox.publish(EVENTS.ResourceSwitcher.ItemSelected, a.href);
        });

        $(window).on('click', hide);

        ko.postbox.subscribe(EVENTS.ResourceSwitcher.Show, () => this.$filter.focus());
        ko.postbox.subscribe(EVENTS.Menu.LevelChanged, hide);
        ko.postbox.subscribe(EVENTS.SearchBox.Show, hide);
    }

    private show() {
        this.$selectDatabaseContainer.addClass('active');
        ko.postbox.publish('ResourceSwitcher.Show');
    }

    private hide() {
        this.$selectDatabaseContainer.removeClass('active');
        ko.postbox.publish('ResourceSwitcher.Hide');
    }
}

export = resourceSwitcher;