/// <reference path="../models/dto.ts" />

import viewModelBase = require("viewmodels/viewModelBase");
import transformer = require("models/transformer");
import getTransformersCommand = require("commands/getTransformersCommand");
import appUrl = require("common/appUrl");
import deleteTransformerConfirm = require("viewmodels/deleteTransformerConfirm");
import dialog = require("plugins/dialog");
import app = require("durandal/app");

//todo: implement refresh from db
class Transformers extends viewModelBase {

    newTransformerUrl = appUrl.forCurrentDatabase().newTransformer;
    
    transformersGroups = ko.observableArray<{ entityName: string; transformers: KnockoutObservableArray<transformer> }>();
    containerSelector ="#transformersContainer";


    constructor() {
        super();
    }

    activate(args) {
        this.fetchTransformers();
        super.activate(args);
    }

    attached() {
        this.createKeyboardShortcut("Alt+N", () => this.navigate(this.newTransformerUrl()), this.containerSelector);
        ko.postbox.publish("SetRawJSONUrl", appUrl.forTransformersRawData(this.activeDatabase()));
    }

    modelPolling() {
        this.fetchTransformers();
    }

    fetchTransformers() {
        new getTransformersCommand(this.activeDatabase())
            .execute()
            .done((transformers: transformerDto[])=> {
                transformers
                    .map(curTransformer=> new transformer().initFromLoad(curTransformer))
                    .forEach(i=> this.putTransformerIntoGroups(i));
            });
    }


    putTransformerIntoGroups(trans: transformer) {
        
        var groupName = trans.name().split("/")[0];
        var group = this.transformersGroups.first(g=> g.entityName === groupName);

        if (group) {
            var existingTrans = group.transformers.first((cur: transformer)=> cur.name() == trans.name());

            if (!existingTrans) {
                group.transformers.push(trans);
            }
        } else {
            this.transformersGroups.push({ entityName: groupName, transformers: ko.observableArray([trans]) });
        }
    }

    collapseAll() {
        $(".index-group-content").collapse('hide');
    }

    expandAll() {
        $(".index-group-content").collapse('show');
    }

    deleteAllTransformers() {
        var allTransformers: transformer[];
        allTransformers = this.getAllTransformers();
        this.promptDeleteTransformers(allTransformers);
    }

    getAllTransformers(): transformer[] {
        var all: transformer[] = [];
        this.transformersGroups().forEach(g => all.pushAll(g.transformers()));
        return all.distinct();
    }


    deleteTransformer(transformerToDelete: transformer) {
        this.promptDeleteTransformers([transformerToDelete]);
    }

    promptDeleteTransformers(transformers: Array<transformer>) {
        var db = this.activeDatabase();
        var deleteViewmodel = new deleteTransformerConfirm(transformers.map(i => i.name()), db);
        deleteViewmodel.deleteTask.done(() => this.removeTransformersFromAllGroups(transformers));
        app.showDialog(deleteViewmodel);
    }

    removeTransformersFromAllGroups(transformers: Array<transformer>) {
        this.transformersGroups().forEach(transGroup => transGroup.transformers.removeAll(transformers));
        this.transformersGroups.remove((item: { entityName: string; transformers: KnockoutObservableArray<transformer> }) => item.transformers().length === 0);
    }
}

export = Transformers;