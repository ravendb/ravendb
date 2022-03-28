import appUrl = require("common/appUrl");
import accessManager = require("common/shell/accessManager");
import getDatabaseRecordCommand = require("commands/resources/getDatabaseRecordCommand");
import saveUnusedDatabaseIDsCommand = require("commands/database/settings/saveUnusedDatabaseIDsCommand");
import changeVectorUtils = require("common/changeVectorUtils");
import getDatabaseStatsCommand from "commands/resources/getDatabaseStatsCommand";
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database from "models/resources/database";

class databaseIDs extends shardViewModelBase {

    view = require("views/database/advanced/databaseIDs.html"); 
    
    usedIDsArray: string[] = [];
    usedIDs = ko.observableArray<string>([]);
    
    idsFromCVsSet = new Set<string>();
    idsFromCVs = ko.observable<Set<string>>(new Set<string>());
    suggestedIDs: KnockoutComputed<string[]>;

    unusedIDs = ko.observableArray<string>([]);
    inputDatabaseID = ko.observable<string>();
    
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
        this.suggestedIDs = ko.pureComputed(() => {
            const idsArray = Array.from(this.idsFromCVs());
            return idsArray.filter(x => !this.usedIDs().includes(x));
        });
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
                    deferred.resolve({can: true});
                } else {
                    const fetchStatsTaskArray = this.fetchAllStatsTasks();
                    const fetchUnusedIDsTask = this.fetchUnusedDatabaseIDs();

                    $.when<any>(...fetchStatsTaskArray, fetchUnusedIDsTask)
                        .then(() => {
                            this.usedIDs(this.usedIDsArray);
                            this.idsFromCVs(this.idsFromCVsSet);
                            deferred.resolve({can: true});
                        })
                        .fail(() => deferred.resolve({redirect: appUrl.forStatus(this.db)}));
                }

                return deferred;
            });
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

    private fetchAllStatsTasks(): JQueryPromise<Raven.Client.Documents.Operations.DatabaseStatistics>[] {
        const locations = this.db.getLocations();
        return locations.map(location => this.fetchStats(location));
    }

    private fetchStats(location: databaseLocationSpecifier): JQueryPromise<Raven.Client.Documents.Operations.DatabaseStatistics> {
        return new getDatabaseStatsCommand(this.db, location)
            .execute()
            .done((stats: Raven.Client.Documents.Operations.DatabaseStatistics) => {

                const changeVector = stats.DatabaseChangeVector.split(",");
                const dbsFromCV = changeVector.map(cvEntry => changeVectorUtils.getDatabaseID(cvEntry));
                dbsFromCV.forEach(x => this.idsFromCVsSet.add(x));

                this.usedIDsArray.push(stats.DatabaseId);
            });
    }
    
    private fetchUnusedDatabaseIDs() {
        return new getDatabaseRecordCommand(this.db)
            .execute()
            .done((document) => {
                this.unusedIDs((document as any)["UnusedDatabaseIds"]);
            });
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
