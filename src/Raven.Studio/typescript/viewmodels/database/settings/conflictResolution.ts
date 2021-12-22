import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import conflictResolutionModel = require("models/database/settings/conflictResolutionModel");
import perCollectionConflictResolutionModel = require("models/database/settings/perCollectionConflictResolutionModel");
import collection = require("models/database/documents/collection");
import conflictResolutionScriptSyntax = require("viewmodels/database/settings/conflictResolutionScriptSyntax");
import getConflictSolverConfigurationCommand = require("commands/database/documents/getConflictSolverConfigurationCommand");
import saveConflictSolverConfigurationCommand = require("commands/database/documents/saveConflictSolverConfigurationCommand");
import eventsCollector = require("common/eventsCollector");

class conflictResolution extends viewModelBase {

    view = require("views/database/settings/conflictResolution.html");

    model = ko.observable<conflictResolutionModel>();

    collections: KnockoutObservableArray<collection>;

    spinners = {
        save: ko.observable<boolean>(false)
    };

    constructor() {
        super();
        aceEditorBindingHandler.install();
        this.bindToCurrentInstance("saveEditedScript", "cancelEditedScript", "editScript", "removeScript", "syntaxHelp", "save");

        this.collections = collectionsTracker.default.collections;
    }

    activate(args: any): JQueryPromise<Raven.Client.ServerWide.ConflictSolver> {
        super.activate(args);

        return new getConflictSolverConfigurationCommand(this.activeDatabase())
            .execute()
            .done(config => {
                if (config) {
                    this.model(new conflictResolutionModel(config));
                } else {
                    const model = conflictResolutionModel.empty();
                    // if configuration isn't available we assume default conflict resolution policy is: resolve to latest
                    model.resolveToLatest(true);
                    this.model(model);
                }
                
                this.initObservables();
            });
    }

    private initObservables() {
        this.dirtyFlag = this.model().dirtyFlag;
    }

    addNewScript() {
        this.model().scriptSelectedForEdit(null);
        this.model().editedScriptSandbox(perCollectionConflictResolutionModel.empty());
    }

    cancelEditedScript() {
        this.model().scriptSelectedForEdit(null);
        this.model().editedScriptSandbox(null);
    }

    save() {
        let hasAnyErrors = false;
        this.spinners.save(true);
        
        eventsCollector.default.reportEvent("conflict-resolution", "save");
        
        const model = this.model();
        
        if (model.editedScriptSandbox()) {
            if (!this.isValid(model.editedScriptSandbox().validationGroup)) {
                hasAnyErrors = true;
            } else {
                this.saveEditedScript();
            }
        }
        
        if (hasAnyErrors) {
            this.spinners.save(false);
            return false;
        }
        
        new saveConflictSolverConfigurationCommand(this.activeDatabase(), model.toDto())
            .execute()
            .done(() => {
                this.model().perCollectionResolvers().forEach(resolver => {
                    resolver.dirtyFlag().reset();
                });
                
                this.dirtyFlag().reset();
            })
            .always(() => this.spinners.save(false));
        
    }

    saveEditedScript() {
        const model = this.model();
        const resolution = model.editedScriptSandbox();
        if (!this.isValid(resolution.validationGroup)) {
            return;
        }

        const newResolutionItem = perCollectionConflictResolutionModel.create(resolution.collection(), resolution.toDto());

        if (!model.scriptSelectedForEdit()) { // it is new
            newResolutionItem.dirtyFlag().forceDirty();
            model.perCollectionResolvers.push(newResolutionItem);
        } else {
            const oldItem = model.scriptSelectedForEdit();

            if (oldItem.dirtyFlag().isDirty() || newResolutionItem.hasUpdates(oldItem)) {
                newResolutionItem.dirtyFlag().forceDirty();
            }

            model.perCollectionResolvers.replace(oldItem, newResolutionItem);
        }

        model.perCollectionResolvers.sort((a, b) => a.collection().toLowerCase().localeCompare(b.collection().toLowerCase()));
        model.editedScriptSandbox(null);
        model.scriptSelectedForEdit(null);
    }

    editScript(script: perCollectionConflictResolutionModel) {
        this.model().scriptSelectedForEdit(script);
        this.model().editedScriptSandbox(perCollectionConflictResolutionModel.create(script.collection(), script.toDto()));
    }

    createCollectionNameAutocompleter(collectionText: KnockoutObservable<string>) {
        return ko.pureComputed(() => {
            let result;
            const key = collectionText();

            const options = collectionsTracker.default.collections().filter(x => !x.isAllDocuments).map(x => x.name);

            const usedOptions = this.model().perCollectionResolvers().map(x => x.collection()).filter(k => k !== key);
            
            const filteredOptions = _.difference(options, usedOptions);

            if (key) {
                result = filteredOptions.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                result = filteredOptions;
            }

            return result;
        });
    }

    removeScript(model: perCollectionConflictResolutionModel) {
        this.model().deleteScript(model);
    }

    syntaxHelp() {
        const viewmodel = new conflictResolutionScriptSyntax();
        app.showBootstrapDialog(viewmodel);
    }

}

export = conflictResolution;
