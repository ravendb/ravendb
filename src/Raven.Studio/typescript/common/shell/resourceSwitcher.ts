
import EVENTS = require("common/constants/events");
import database = require("models/resources/database");
import resourcesManager = require("common/shell/resourcesManager");
import appUrl = require("common/appUrl");

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

    private databases: KnockoutComputed<database[]>;
    private resourcesManager = resourcesManager.default;

    private readonly hideHandler = (e: Event) => {
        if (this.shouldConsumeHideEvent(e)) {
            this.hide()
        }
    };

    filter = ko.observable<string>();
    filteredDatabases: KnockoutComputed<database[]>;

    constructor() {
        this.filteredDatabases = ko.computed(() => {
            const filter = this.filter();
            const databases = this.resourcesManager.databases();

            if (!filter)
                return databases;

            if (!databases) {
                return [];
            }

            return databases.filter(x => x.name.toLowerCase().includes(filter.toLowerCase()));
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
            if (this.$selectDatabaseContainer.is('.active')) {
                this.hide();
            } else {
                this.show();
            }

            e.stopPropagation();
        });

        const self = this;
        $('.box-container a', this.$selectDatabaseContainer).on('click', function (e: Event) {
            e.stopPropagation();
            self.hide();
            let a: HTMLAnchorElement = this as HTMLAnchorElement;
            ko.postbox.publish(EVENTS.ResourceSwitcher.ItemSelected, a.href);
        });
    }

    selectDatabase(rs: database, $event: JQueryEventObject) {
        if ($event.ctrlKey) {
            window.open(appUrl.forDocumentsByDatabaseName(null, rs.name));
        } else {
            rs.activate();
            this.hide();
        }
    }

    private show() {
        window.addEventListener("click", this.hideHandler, true);

        this.$selectDatabaseContainer.addClass('active');
        this.$filter.focus();
    }

    private hide() {
        window.removeEventListener("click", this.hideHandler, true);

        this.$selectDatabaseContainer.removeClass('active');
    }

    private shouldConsumeHideEvent(e: Event) {
        return $(e.target).parents(".resource-switcher-container").length === 0
            && !$(e.target).hasClass(".resource-switcher");
    }

}

export = resourceSwitcher;