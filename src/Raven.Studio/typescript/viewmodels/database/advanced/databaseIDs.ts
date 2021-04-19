import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import accessManager = require("common/shell/accessManager");
import getDatabaseDetailedStatsCommand = require("commands/resources/getDatabaseDetailedStatsCommand");
import getDatabaseRecordCommand = require("commands/resources/getDatabaseRecordCommand");
import saveUnusedDatabaseIDsCommand = require("commands/database/settings/saveUnusedDatabaseIDsCommand");
import changeVectorUtils = require("common/changeVectorUtils");

class databaseIDs extends viewModelBase {

    isForbidden = ko.observable<boolean>(false);
    
    databaseID = ko.observable<string>();
    databaseChangeVector = ko.observableArray<string>([]);
    
    unusedDatabaseIDs = ko.observableArray<string>([]);
    inputDatabaseId = ko.observable<string>();

    isSaveEnabled = ko.observable<boolean>();

    spinners = {
        save: ko.observable<boolean>(false)
    };

    constructor() {
        super();
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
                        .fail(() => deferred.resolve({ redirect: appUrl.forStatus(this.activeDatabase()) }));
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
        return new getDatabaseDetailedStatsCommand(this.activeDatabase())
            .execute()
            .done((stats: Raven.Client.Documents.Operations.DetailedDatabaseStatistics) => {
                this.databaseChangeVector(stats.DatabaseChangeVector.split(","));
                this.databaseID(stats.DatabaseId);
            });
    }
    
    private fetchUnusedDatabaseIDs() {
        return new getDatabaseRecordCommand(this.activeDatabase())
            .execute()
            .done((document) => {
                this.unusedDatabaseIDs((document as any)["UnusedDatabaseIds"]);
            });
    }

    saveUnusedDatabaseIDs() {
        this.spinners.save(true);
        
        new saveUnusedDatabaseIDsCommand(this.unusedDatabaseIDs(), this.activeDatabase().name)
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
