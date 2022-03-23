import appUrl = require("common/appUrl");
import accessManager = require("common/shell/accessManager");
import getDatabaseRecordCommand = require("commands/resources/getDatabaseRecordCommand");
import saveUnusedDatabaseIDsCommand = require("commands/database/settings/saveUnusedDatabaseIDsCommand");
import changeVectorUtils = require("common/changeVectorUtils");
import getDatabaseStatsCommand from "commands/resources/getDatabaseStatsCommand";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database from "models/resources/database";

class databaseIDs extends shardViewModelBase {

    view = require("views/database/advanced/databaseIDs.html");

    isForbidden = ko.observable<boolean>(false);
    
    databaseID = ko.observable<string>();
    databaseChangeVector = ko.observableArray<string>([]);
    
    unusedDatabaseIDs = ko.observableArray<string>([]);
    inputDatabaseId = ko.observable<string>();

    isSaveEnabled = ko.observable<boolean>();

    spinners = {
        save: ko.observable<boolean>(false)
    };

    constructor(db: database) {
        super(db);
        this.bindToCurrentInstance("addToUnusedList", "removeFromUnusedList");
    }

    compositionComplete() {
        super.compositionComplete();
        this.setupDisableReasons();
    }
    
    canAddIdToUnusedIDs(cvEntry: string) {
       return ko.pureComputed(() => changeVectorUtils.getDatabaseID(cvEntry) !== this.databaseID());
    }

    itemIsInsideUnusedList(cvEntry: string) {
        return ko.pureComputed(() => {
            const idPart = changeVectorUtils.getDatabaseID(cvEntry);
            return _.includes(this.unusedDatabaseIDs(), idPart);
        });
    }

    canActivate(args: any) {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                const deferred = $.Deferred<canActivateResultDto>();

                this.isForbidden(!accessManager.default.isOperatorOrAbove());
                
                if (this.isForbidden()) {
                    deferred.resolve({ can: true });
                } else {
                    const fetchStatsTask = this.fetchStats();
                    const fetchUnusedIDsTask = this.fetchUnusedDatabaseIDs();

                    return $.when<any>(fetchStatsTask, fetchUnusedIDsTask)
                        .then(() => deferred.resolve({ can: true }))
                        .fail(() => deferred.resolve({ redirect: appUrl.forStatus(this.db) }));
                }

                return deferred;
            });
    }

    activate(args: any) {
        super.activate(args);
        
        this.dirtyFlag = new ko.DirtyFlag([this.unusedDatabaseIDs]);
        
        this.isSaveEnabled = ko.pureComputed<boolean>(() => {
            const dirty = this.dirtyFlag().isDirty();
            const saving = this.spinners.save();
            return dirty && !saving;
        });
    }

    private fetchStats(): JQueryPromise<Raven.Client.Documents.Operations.DatabaseStatistics> {
        const db = this.db;
        const localNode = clusterTopologyManager.default.localNodeTag();
        return new getDatabaseStatsCommand(db, db.getFirstLocation(localNode))
            .execute()
            .done((stats: Raven.Client.Documents.Operations.DatabaseStatistics) => {
                this.databaseChangeVector(stats.DatabaseChangeVector.split(","));
                this.databaseID(stats.DatabaseId);
            });
    }
    
    private fetchUnusedDatabaseIDs() {
        return new getDatabaseRecordCommand(this.db)
            .execute()
            .done((document) => {
                this.unusedDatabaseIDs((document as any)["UnusedDatabaseIds"]);
            });
    }

    saveUnusedDatabaseIDs() {
        this.spinners.save(true);
        
        new saveUnusedDatabaseIDsCommand(this.unusedDatabaseIDs(), this.db.name)
            .execute()
            .done(() => this.dirtyFlag().reset())
            .always(() => this.spinners.save(false));
    }

    addInputToUnusedList() {
        this.addWithBlink(this.inputDatabaseId());
    }
    
    addToUnusedList(cvEntry: string) {
        const dbId = changeVectorUtils.getDatabaseID(cvEntry);
        this.addWithBlink(dbId);
    }
    
    private addWithBlink(dbIdToAdd: string) {
        if (!_.includes(this.unusedDatabaseIDs(), dbIdToAdd)) {
            this.unusedDatabaseIDs.unshift(dbIdToAdd);
            $(".collection-list li").first().addClass("blink-style");
        }
    }
    
    removeFromUnusedList(dbId: string) {
        this.unusedDatabaseIDs.remove(dbId);
    }
}

export = databaseIDs;
