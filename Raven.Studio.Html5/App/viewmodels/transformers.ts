/// <reference path="../models/dto.ts" />

import viewModelBase = require("viewmodels/viewModelBase");
import transformer = require("models/transformer");
import getTransformersCommand = require("commands/getTransformersCommand");
import appUrl = require("common/appUrl");

//todo: implement refresh from db
class Transformers extends viewModelBase {

    newTransformerUrl = appUrl.forCurrentDatabase().newTransformer;
    
    //transformersList = ko.observableArray<transformer>();
    groupedByIds = ko.observable<Boolean>();

    
    transformersGroups = ko.observableArray<{ entityName: string; transformers: KnockoutObservableArray<transformer> }>();
    
    
    constructor() {
        super();
    }

    activate(args) {
        super.activate(args);
        this.fetchTransformers();
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
                .forEach(i => this.putTransformerIntoGroups(i));
        });
    }




    putTransformerIntoGroups(trans: transformer) {
        var groupName = trans.name().split("/")[0];
        var group = this.transformersGroups.first(g => g.entityName === groupName);

        if (group) {
            var existingTrans = group.transformers.first((cur: transformer) => cur.name() == trans.name());
            
            if (!existingTrans) {
                group.transformers.push(trans);
            }
        } else {
            this.transformersGroups.push({ entityName: groupName, transformers: ko.observableArray([trans]) });
        }
    }
    processTransformer

    toggleGrouping() {

    }

    collapseAll() {

    }

    expandAll() {

    }
    deleteAllTransformers() {
        
    }

    deleteTransformer() {
        
    }

}

export = Transformers;