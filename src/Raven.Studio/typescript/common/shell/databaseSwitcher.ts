
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

    private databasesManager = databasesManager.default;

    highlightedItem = ko.observable<string>(null);

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
            
            this.autoHighlight();
        });

        this.$selectDatabase.on('click', (e) => {
            if (this.$selectDatabaseContainer.is('.active')) {
                this.hide();
            } else {
                this.show();
                
                this.autoHighlight();
            }

            e.stopPropagation();
        });

        this.filter.subscribe(() => {
            setTimeout(() => this.autoHighlight(), 1);
        });
        
        const self = this;
        $('.box-container', this.$selectDatabaseContainer).on('click', "a", function (e: Event) {
            e.stopPropagation();
            self.hide();
            let a: HTMLAnchorElement = this as HTMLAnchorElement;
            ko.postbox.publish(EVENTS.DatabaseSwitcher.ItemSelected, a.href);
        });
        
        this.$filter.keydown(e => {
            if (e.key === "ArrowDown" || e.key === "ArrowUp") {
                this.changeHighlightedItem(e.key === "ArrowDown" ? "down" : "up");
                return false; // prevent default
            } else if (e.key === "Enter") {
                this.dispatchGoToDatabase();
                return false;
            }

            return true;
        });
    }

    /**
     * Highlight active database (if on list) or first item
     */
    private autoHighlight() {
        const currentDatabase = this.databasesManager.activeDatabaseTracker.database();
        
        const filteredDatabases = this.filteredDatabases();
        
        const matchedDatabase = currentDatabase ? filteredDatabases.find(x => x.name === currentDatabase.name) : null; 
        
        if (matchedDatabase) {
            this.highlightedItem(matchedDatabase.name);
        } else if (filteredDatabases.length) {
            this.highlightedItem(filteredDatabases[0].name);
        } else {
            this.highlightedItem(null);
        }
    }
    
    private changeHighlightedItem(direction: "up" | "down") {
        const enabledRelevantDatabases = this.filteredDatabases().filter(x => !x.disabled() && x.relevant());

        const currentName = this.highlightedItem();
        const currentIdx = enabledRelevantDatabases.findIndex(x => x.name === currentName);
        
        const indexToUse = currentIdx === -1 ?
            (direction === "down" ? -1 : enabledRelevantDatabases.length) :
            currentIdx;

        const nextIdx = direction === "down" ?
            (indexToUse + 1) % enabledRelevantDatabases.length :
            ((indexToUse - 1) + enabledRelevantDatabases.length) % enabledRelevantDatabases.length;

        this.highlightedItem(enabledRelevantDatabases[nextIdx].name);
    }

    private dispatchGoToDatabase() {
        const highlight = this.highlightedItem();
        if (highlight) {
            const db = this.filteredDatabases().find(x => x.name === highlight);
            if (db) {
                this.highlightedItem(null);
                this.selectDatabase(db);
            }
        }
    }

    selectDatabase(db: database, $event?: JQueryEventObject) {
        if ($event && $event.ctrlKey) {
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
