/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");
import perCollectionConflictResolutionModel = require("models/database/settings/perCollectionConflictResolutionModel");

class conflictResolutionModel {

    perCollectionResolvers = ko.observableArray<perCollectionConflictResolutionModel>([]);
    resolveToLatest = ko.observable<boolean>(false);

    scriptSelectedForEdit = ko.observable<perCollectionConflictResolutionModel>();
    editedScriptSandbox = ko.observable<perCollectionConflictResolutionModel>();

    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.ServerWide.ConflictSolver) {
        this.update(dto);
        this.initializeObservables();

        _.bindAll(this, "useCollection");
    }

    initializeObservables() {
        const innerDirtyFlag = ko.pureComputed(() => !!this.editedScriptSandbox() && this.editedScriptSandbox().dirtyFlag().isDirty());
        
        const scriptsCount = ko.pureComputed(() => this.perCollectionResolvers().length);
        
        const hasAnyDirtyScript = ko.pureComputed(() => {
            let anyDirty = false;
            this.perCollectionResolvers().forEach(script => {
                if (script.dirtyFlag().isDirty()) {
                    anyDirty = true;
                    // don't break here - we want to track all dependencies
                }    
            });
            
            return anyDirty;
        });
        
        this.dirtyFlag = new ko.DirtyFlag([
            innerDirtyFlag,
            scriptsCount,
            hasAnyDirtyScript,
            this.resolveToLatest
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    useCollection(collectionToUse: string) {
        this.editedScriptSandbox().collection(collectionToUse);
    }
    
    protected update(dto: Raven.Client.ServerWide.ConflictSolver) {
        this.resolveToLatest(dto.ResolveToLatest);

        const mappedResolutions = _.map(dto.ResolveByCollection, (value, key) => {
            return perCollectionConflictResolutionModel.create(key, value);
        });


        this.perCollectionResolvers(mappedResolutions);
    }

    toDto(): Raven.Client.ServerWide.ConflictSolver {
        const perCollectionScripts = {} as dictionary<Raven.Client.ServerWide.ScriptResolver>;

        this.perCollectionResolvers().forEach(resolver => {
            perCollectionScripts[resolver.collection()] = resolver.toDto()
        });

        return {
            ResolveToLatest: this.resolveToLatest(),
            ResolveByCollection: perCollectionScripts
        }
    }

    deleteScript(script: perCollectionConflictResolutionModel) {
        this.perCollectionResolvers.remove(script);

        if (this.scriptSelectedForEdit() === script) {
            this.editedScriptSandbox(null);
            this.scriptSelectedForEdit(null);
        }
    }

    static empty(): conflictResolutionModel {
        return new conflictResolutionModel({
            ResolveToLatest: false,
            ResolveByCollection: {}
        });
    }
}

export = conflictResolutionModel;
