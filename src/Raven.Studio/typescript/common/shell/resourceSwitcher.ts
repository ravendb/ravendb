
import EVENTS = require("common/constants/events");
import resource = require("models/resources/resource");
import resourcesManager = require("common/shell/resourcesManager");
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

    private resources: KnockoutComputed<resource[]>;
    private resourcesManager = resourcesManager.default;

    filter = ko.observable<string>();
    filteredResources: KnockoutComputed<resource[]>;

    constructor() {
        this.filteredResources = ko.computed(() => {
            const filter = this.filter();
            const resources = this.resourcesManager.resources();

            if (!filter)
                return resources;

            if (!resources) {
                return [];
            }

            return resources.filter(x => x.name.toLowerCase().contains(filter.toLowerCase()));
        });
    }

    initialize() {
        this.$selectDatabaseContainer = $('.resource-switcher-container');
        this.$selectDatabase = $('.form-control.btn-toggle.resource-switcher');
        this.$filter = $('.resource-switcher-container .database-filter');

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

        const hide = () => this.hide();

        $('.box-container a', this.$selectDatabaseContainer).on('click', function (e) {
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

    selectResource(rs: resource) {
        rs.activate();
    }

    private show() {
        this.$selectDatabaseContainer.addClass('active');
        ko.postbox.publish(EVENTS.ResourceSwitcher.Show);
    }

    private hide() {
        this.$selectDatabaseContainer.removeClass('active');
        ko.postbox.publish(EVENTS.ResourceSwitcher.Hide);
    }
}

export = resourceSwitcher;