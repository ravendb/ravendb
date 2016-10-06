import viewModelBase = require("viewmodels/viewModelBase");
import transformer = require("models/database/index/transformer");
import getTransformersCommand = require("commands/database/transformers/getTransformersCommand");
import saveTransformerLockModeCommand = require("commands/database/transformers/saveTransformerLockModeCommand");
import appUrl = require("common/appUrl");
import deleteTransformerConfirm = require("viewmodels/database/transformers/deleteTransformerConfirm");
import app = require("durandal/app");
import changeSubscription = require("common/changeSubscription");
import changesContext = require("common/changesContext");
import database = require("models/resources/database");
import copyTransformerDialog = require("viewmodels/database/transformers/copyTransformerDialog");

type transformerGroup = {
    entityName: string;
    transformers: KnockoutObservableArray<transformer>;
    groupHidden: KnockoutObservable<boolean>;
}

class transformers extends viewModelBase {

    newTransformerUrl = appUrl.forCurrentDatabase().newTransformer;
    transformersGroups = ko.observableArray<transformerGroup>();
    selectedTransformersName = ko.observableArray<string>();
    searchText = ko.observable<string>();
    lockModeCommon: KnockoutComputed<string>;
    selectionState: KnockoutComputed<checkbox>;

    constructor() {
        super();
        this.initObservables();
    }

    private initObservables() {
        this.searchText.throttle(200).subscribe(() => this.filterTransformers());

        this.lockModeCommon = ko.computed(() => {
            const selectedTransformers = this.getSelectedTransformers();
            if (selectedTransformers.length === 0)
                return "None";

            const firstLockMode = selectedTransformers[0].lockMode();
            for (let i = 1; i < selectedTransformers.length; i++) {
                if (selectedTransformers[i].lockMode() !== firstLockMode) {
                    return "Mixed";
                }
            }
            return firstLockMode;
        });

        this.selectionState = ko.pureComputed<checkbox>(() => {
            var selectedCount = this.selectedTransformersName().length;
            if (selectedCount === this.getAllTransformers().length)
                return checkbox.Checked;
            if (selectedCount > 0)
                return checkbox.SomeChecked;
            return checkbox.UnChecked;
        });
    }

    activate(args: any) {
        return this.fetchTransformers(this.activeDatabase());
    }

    attached() {
        super.attached();
        this.updateHelpLink("OWRJLV");
        ko.postbox.publish("SetRawJSONUrl", appUrl.forTransformersRawData(this.activeDatabase()));
    }

    private filterTransformers() {
        const filterLower = this.searchText().toLowerCase();

        this.transformersGroups()
            .forEach(transformerGroup => {
                let hasAnyInGroup = false;

                transformerGroup.transformers().forEach(trans => {
                    const match = trans.name().toLowerCase().indexOf(filterLower) >= 0;
                    trans.filteredOut(!match);

                    if (match) {
                        hasAnyInGroup = true;
                    }
                });

                transformerGroup.groupHidden(!hasAnyInGroup);
            });
    }

    private fetchTransformers(db: database) {
        return new getTransformersCommand(db)
            .execute()
            .done((transformers: Raven.Abstractions.Indexing.TransformerDefinition[]) => {
                transformers
                    .map(t => new transformer(t))
                    .forEach(i => this.putTransformerIntoGroup(i));
            });
    }

    createNotifications(): Array<changeSubscription> {
        return [
            //TODO: use cooldown changesContext.currentResourceChangesApi().watchAllTransformers((e: Raven.Abstractions.Data.TransformerChangeNotification) => this.processTransformerEvent(e))
        ];
    }

    private processTransformerEvent(e: Raven.Abstractions.Data.TransformerChangeNotification) {
        if (e.Type === "TransformerRemoved") {
            const existingTransformer = this.findTransformerByName(e.Name);
            if (existingTransformer) {
                this.removeTransformersFromAllGroups([existingTransformer]);    
            }
        } else {
            setTimeout(() => {
                this.fetchTransformers(this.activeDatabase());
            }, 5000); //TODO: do we need such timeout here?
        }
    }

    findTransformerByName(transformerName: string): transformer {
        const transformsGroups = this.transformersGroups();
        for (let i = 0; i < transformsGroups.length; i++) {
            const group = transformsGroups[i];

            const transformers = group.transformers();
            for (let j = 0; j < transformers.length; j++) {
                if (transformers[j].name() === transformerName) {
                    return transformers[j];
                }
            }
        }

        return null;
    }

    putTransformerIntoGroup(trans: transformer) {
        const groupName = trans.name().split("/")[0];
        const group = this.transformersGroups.first(g => g.entityName === groupName);

        if (group) {
            const existingTrans = group.transformers.first((cur: transformer) => cur.name() === trans.name());

            if (!existingTrans) {
                group.transformers.push(trans);
            }

        } else {
            this.transformersGroups.push({
                entityName: groupName,
                transformers: ko.observableArray([trans]),
                groupHidden: ko.observable(false)
            });
        }
    }

    deleteSelectedTransformers() {
        this.promptDeleteTransformers(this.getSelectedTransformers());
    }

    private getAllTransformers(): transformer[] {
        var all: transformer[] = [];
        this.transformersGroups().forEach(g => all.pushAll(g.transformers()));
        return all.distinct();
    }

    private getSelectedTransformers(): Array<transformer> {
        const selectedTransformers = this.selectedTransformersName();
        return this.getAllTransformers().filter(x => selectedTransformers.contains(x.name()));
    }

    deleteTransformer(transformerToDelete: transformer) {
        this.promptDeleteTransformers([transformerToDelete]);
    }

    pasteTransformer() { //TODO: move to different controller
        app.showDialog(new copyTransformerDialog("", this.activeDatabase(), true));
    }

    copyTransformer(t: transformer) {
        //TODO: move to different controller
        app.showDialog(new copyTransformerDialog(t.name(), this.activeDatabase(), false));
    }

    copySelectedTransformers() {
        alert("implement me!"); //TODO:
    }

    private promptDeleteTransformers(transformers: Array<transformer>) {
        const db = this.activeDatabase();
        const deleteViewmodel = new deleteTransformerConfirm(transformers.map(i => i.name()), db);
        deleteViewmodel.deleteTask.done(() => this.removeTransformersFromAllGroups(transformers));
        app.showDialog(deleteViewmodel);
    }

    private removeTransformersFromAllGroups(transformers: Array<transformer>) {
        this.transformersGroups().forEach(transGroup => transGroup.transformers.removeAll(transformers));
        this.transformersGroups.remove((item: transformerGroup) => item.transformers().length === 0);
    }

    setLockModeSelectedTransformers(lockModeString: Raven.Abstractions.Indexing.TransformerLockMode,
        localModeString: string) {

        if (this.lockModeCommon() === lockModeString)
            return;

        const lockModeTitle = `Do you want to ${localModeString} selected transformers?`;

        //TODO: create dialog: const indexLockAllVm = new indexLockSelectedConfirm(lockModeString, this.activeDatabase(), this.getSelectedTransformers(), lockModeTitle);
        //TODO: app.showDialog(indexLockAllVm);
    }

    lockTransformer(t: transformer) {
        this.updateTransformerLockMode(t, "LockedIgnore");
    }

    unlockTransformer(t: transformer) {
        this.updateTransformerLockMode(t, "Unlock");
    }

    private updateTransformerLockMode(t: transformer, lockMode: Raven.Abstractions.Indexing.TransformerLockMode) {
        const originalLockMode = t.lockMode();
        if (originalLockMode !== lockMode) {
            t.lockMode(lockMode);

            new saveTransformerLockModeCommand(t.name(), lockMode, this.activeDatabase())
                .execute()
                .fail(() => t.lockMode(originalLockMode));
        }
    }

    toggleSelectAll() {
        const selectedCount = this.selectedTransformersName().length;

        if (selectedCount > 0) {
            this.selectedTransformersName([]);
        } else {
            const allTransformerNames = this.getAllTransformers().map(idx => idx.name());
            this.selectedTransformersName(allTransformerNames);
        }
    }
}

export = transformers;
