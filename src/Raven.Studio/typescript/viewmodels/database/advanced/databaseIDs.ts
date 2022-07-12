import appUrl = require("common/appUrl");
import accessManager = require("common/shell/accessManager");
import getDatabaseRecordCommand = require("commands/resources/getDatabaseRecordCommand");
import saveUnusedDatabaseIDsCommand = require("commands/database/settings/saveUnusedDatabaseIDsCommand");
import changeVectorUtils = require("common/changeVectorUtils");
import getDatabaseStatsCommand from "commands/resources/getDatabaseStatsCommand";
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database from "models/resources/database";

interface NodeStats {
    databaseId: string;
    databaseIdsFromChangeVector: string[];
}

class databaseIDs extends shardViewModelBase {

    view = require("views/database/advanced/databaseIDs.html");

    unusedIDs = ko.observableArray<string>([]);
    
    usedIDs = ko.observableArray<string>([]);
    idsFromCVs = ko.observableArray<string>();
    
    inputDatabaseID = ko.observable<string>();

    suggestedIDs: KnockoutComputed<string[]>;
    
    isForbidden = ko.observable<boolean>(false);
    isSaveEnabled = ko.observable<boolean>();

    spinners = {
        save: ko.observable<boolean>(false)
    };

    constructor(db: database) {
        super(db);
        this.bindToCurrentInstance("addToUnusedList", "removeFromUnusedList");
        this.initObservables();
    }
    
    private initObservables(): void {
        this.suggestedIDs = ko.pureComputed(() => this.idsFromCVs().filter(x => !this.usedIDs().includes(x)));
    }

    compositionComplete() {
        super.compositionComplete();
        this.setupDisableReasons();
    }

    itemIsInsideUnusedList(dbId: string) {
        return ko.pureComputed(() => this.unusedIDs().includes(dbId));
    }

    canActivate(args: any) {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                const deferred = $.Deferred<canActivateResultDto>();

                this.isForbidden(!accessManager.default.isOperatorOrAbove());
                
                if (this.isForbidden()) {
                    deferred.resolve({ can: true });
                } else {
                    this.loadData()
                        .then(() => deferred.resolve({ can: true }))
                        .catch(() => deferred.resolve({ redirect: appUrl.forStatus(this.db) }));
                }

                return deferred;
            });
    }
    
    private async loadData(): Promise<void> {
        const fetchUnusedIDsTask = this.fetchUnusedDatabaseIDs();

        const nodeStats = await Promise.all(this.fetchAllStatsTasks());
        
        const unusedIds = await fetchUnusedIDsTask;
        
        const usedIdsSet = new Set(nodeStats.map(x => x.databaseId));
        const idsFromChangeVectors = new Set(nodeStats.flatMap(x => x.databaseIdsFromChangeVector));
        
        this.unusedIDs(unusedIds);
        this.usedIDs(Array.from(usedIdsSet));
        this.idsFromCVs(Array.from(idsFromChangeVectors));
    }
    
    activate(args: any) {
        super.activate(args);
        
        this.dirtyFlag = new ko.DirtyFlag([this.unusedIDs]);
        
        this.isSaveEnabled = ko.pureComputed<boolean>(() => {
            const dirty = this.dirtyFlag().isDirty();
            const saving = this.spinners.save();
            return dirty && !saving;
        });
    }

    private fetchAllStatsTasks(): Promise<NodeStats>[] {
        const locations = this.db.getLocations();
        return locations.map(location => this.fetchStats(location));
    }

    private async fetchStats(location: databaseLocationSpecifier): Promise<NodeStats> {
        const stats = await new getDatabaseStatsCommand(this.db, location)
            .execute();
        
        if (!stats.DatabaseChangeVector) {
            return {
                databaseId: stats.DatabaseId,
                databaseIdsFromChangeVector: []
            }
        }
        
        const changeVector = stats.DatabaseChangeVector.split(",");
        const dbsFromCV = changeVector.map(cvEntry => changeVectorUtils.getDatabaseID(cvEntry));

        return {
            databaseId: stats.DatabaseId,
            databaseIdsFromChangeVector: dbsFromCV
        }
    }
    
    private async fetchUnusedDatabaseIDs(): Promise<string[]> {
        const document = await new getDatabaseRecordCommand(this.db)
            .execute();

        return (document as any)["UnusedDatabaseIds"];
    }

    saveUnusedDatabaseIDs() {
        this.spinners.save(true);
        
        new saveUnusedDatabaseIDsCommand(this.unusedIDs(), this.db.name)
            .execute()
            .done(() => this.dirtyFlag().reset())
            .always(() => this.spinners.save(false));
    }

    addInputToUnusedList() {
        this.addWithBlink(this.inputDatabaseID());
    }
    
    addToUnusedList(dbID: string) {
        this.addWithBlink(dbID);
    }
    
    private addWithBlink(dbIdToAdd: string) {
        if (!this.unusedIDs().includes(dbIdToAdd)) {
            this.unusedIDs.unshift(dbIdToAdd);
            $(".collection-list li").first().addClass("blink-style");
        }
    }
    
    removeFromUnusedList(dbId: string) {
        this.unusedIDs.remove(dbId);
    }
}

export = databaseIDs;
