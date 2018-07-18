
import EVENTS = require("common/constants/events");
import database = require("models/resources/database");
import databasesManager = require("common/shell/databasesManager");
import appUrl = require("common/appUrl");

/*
    Events emitted through ko.postbox
        * DatabaseSwitcher.Show - when searchbox is opened
        * DatabaseSwitcher.Hide - when searchbox is hidden
        * DatabaseSwitcher.ItemSelected - item selected from database switcher pane
*/
class databaseSwitcher {

    private $selectDatabaseContainer: JQuery;
    private $selectDatabase: JQuery;
    private $filter: JQuery;

    private databases: KnockoutComputed<database[]>;
    private databasesManager = databasesManager.default;

    private readonly hideHandler = (e: Event) => {
        if (this.shouldConsumeHideEvent(e)) {
            this.hide();
        }
    };

    filter = ko.observable<string>();
    filteredDatabases: KnockoutComputed<database[]>;

    constructor() {
        this.filteredDatabases = ko.pureComputed(() => {
            const filter = this.filter();
            const databases = this.databasesManager.databases();

            if (!filter)
                return databases;

            if (!databases) {
                return [];
            }

            return databases.filter(x => x.name.toLowerCase().includes(filter.toLowerCase()));
        });
    }

    initialize() {
        this.$selectDatabaseContainer = $('.database-switcher-container');
        this.$selectDatabase = $('.form-control.btn-toggle.database-switcher');
        this.$filter = $('.database-switcher-container .database-filter');

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
            ko.postbox.publish(EVENTS.DatabaseSwitcher.ItemSelected, a.href);
        });
    }

    selectDatabase(db: database, $event: JQueryEventObject) {
        if ($event.ctrlKey) {
            window.open(appUrl.forDocumentsByDatabaseName(null, db.name));
        } else {
            this.databasesManager.activate(db);
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
        if ($(e.target).parents(".resources-link").length) {
            e.stopPropagation();
            return true;
        }
        
        return $(e.target).parents(".database-switcher-container").length === 0
            && !$(e.target).hasClass(".database-switcher");
    }

}

export = databaseSwitcher;
