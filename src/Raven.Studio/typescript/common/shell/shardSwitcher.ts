
import EVENTS = require("common/constants/events");
import database = require("models/resources/database");
import databasesManager = require("common/shell/databasesManager");
import appUrl = require("common/appUrl");
import shardedDatabase from "models/resources/shardedDatabase";
import databaseShard from "models/resources/databaseShard";

/*
    Events emitted through ko.postbox
        * ShardSwitcher.Show - when searchbox is opened
        * ShardSwitcher.Hide - when searchbox is hidden
        * ShardSwitcher.ItemSelected - item selected from database switcher pane
*/
class shardSwitcher {

    private $selectShardContainer: JQuery;
    private $selectShard: JQuery;

    private databasesManager = databasesManager.default;

    highlightedItem = ko.observable<string>(null);

    shards: KnockoutComputed<databaseShard[]>;

    shardedDatabaseGroup: KnockoutComputed<shardedDatabase>;

    constructor() {
        this.shards = ko.pureComputed(() => {
            const currentDb = this.databasesManager.activeDatabaseTracker.database();

            if (!currentDb) {
                return [];
            }
            
            if (currentDb.group instanceof shardedDatabase) {
                return currentDb.group.shards();
            }
            
            return [];
        });
        
        this.shardedDatabaseGroup = ko.pureComputed(() => {
            const currentDb = this.databasesManager.activeDatabaseTracker.database();
            if (!currentDb) {
                return null;
            }
            if (currentDb.group instanceof shardedDatabase) {
                return currentDb.group;
            }
            
            return null;
        });
    }
    
    private readonly hideHandler = (e: Event) => {
        if (this.shouldConsumeHideEvent(e)) {
            this.hide();
        }
    };

    initialize() {
        this.$selectShardContainer = $('.shard-switcher-container');
        this.$selectShard = $('.form-control.btn-toggle.shard-switcher');

        this.$selectShardContainer.on('click', (e) => {
            e.stopPropagation();
            this.show();
            
            this.autoHighlight();
        });

        this.$selectShard.on('click', (e) => {
            if (this.$selectShardContainer.is('.active')) {
                this.hide();
            } else {
                this.show();
                
                this.autoHighlight();
            }

            e.stopPropagation();
        });

        const self = this;
        $('.box-container', this.$selectShardContainer).on('click', "a", function (e: Event) {
            e.stopPropagation();
            self.hide();
            let a: HTMLAnchorElement = this as HTMLAnchorElement;
            ko.postbox.publish(EVENTS.ShardSwitcher.ItemSelected, a.href);
        });
    }

    /**
     * Highlight active shard
     */
    private autoHighlight() {
        const currentDatabase = this.databasesManager.activeDatabaseTracker.database();
        const highlightedName = currentDatabase ? currentDatabase.name : null;
        this.highlightedItem(highlightedName);
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

        this.$selectShardContainer.addClass('active');
    }

    private hide() {
        window.removeEventListener("click", this.hideHandler, true);

        this.$selectShardContainer.removeClass('active');
    }

    private shouldConsumeHideEvent(e: Event) {
        if ($(e.target).parents(".resources-link").length) {
            e.stopPropagation();
            return true;
        }
        
        return $(e.target).parents(".shard-switcher-container").length === 0
            && !$(e.target).hasClass(".shard-switcher");
    }

}

export = shardSwitcher;
