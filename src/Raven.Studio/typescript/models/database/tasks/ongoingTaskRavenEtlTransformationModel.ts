/// <reference path="../../../../typings/tsd.d.ts"/>
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import jsonUtil = require("common/jsonUtil");

class ongoingTaskEtlTransformationModel {
    name = ko.observable<string>();
    script = ko.observable<string>();
    transformScriptCollections = ko.observableArray<string>([]);

    applyScriptForAllCollections = ko.observable<boolean>(false);
    isNew = ko.observable<boolean>(true);
    inputCollection = ko.observable<string>();
    canAddCollection: KnockoutComputed<boolean>;

    validationGroup: KnockoutValidationGroup; 
    
    dirtyFlag: () => DirtyFlag;
  
    constructor(dto: Raven.Client.ServerWide.ETL.Transformation, isNew: boolean) {
        this.update(dto, isNew);
        this.initObservables();
        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([this.name,
                this.script,
                this.transformScriptCollections],
            false, jsonUtil.newLineNormalizingHashFunction);
    }

    getCollectionEntry(collectionName: string) {
        return collectionsTracker.default.getCollectionColorIndex(collectionName);        
    }

    static empty(): ongoingTaskEtlTransformationModel {
        return new ongoingTaskEtlTransformationModel(
            {
                ApplyToAllDocuments: false, 
                Collections: [],
                Disabled: false,
                HasLoadAttachment: false,
                Name: "",
                Script: ""
            }, true);
    }

    toDto(): Raven.Client.ServerWide.ETL.Transformation {
        return {
            ApplyToAllDocuments: this.applyScriptForAllCollections(),
            Collections: this.applyScriptForAllCollections() ? null : this.transformScriptCollections(),
            Disabled: false,
            HasLoadAttachment: false,
            Name: this.name(),
            Script: this.script()
        }
    }

    private initObservables() {
        this.canAddCollection = ko.pureComputed(() => {
            const collectionToAdd = this.inputCollection();
            return collectionToAdd && !this.transformScriptCollections().find(x => x === collectionToAdd);
        });
    }

    private initValidation() {
        this.name.extend({
            required: true
        });
        
        this.transformScriptCollections.extend({
            validation: [
                {
                    validator: () => this.applyScriptForAllCollections() || this.transformScriptCollections().length > 0,
                    message: "All least one collection is required"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            name: this.name,
            transformScriptCollections: this.transformScriptCollections
        });
    }

    removeCollection(collection: string) {
        this.transformScriptCollections.remove(collection);
        this.applyScriptForAllCollections(false);
    }

    addCollection() {
        this.addWithBlink(this.inputCollection());
    }
    
    addWithBlink(collectionName: string) {
        this.transformScriptCollections.unshift(collectionName);
        this.inputCollection("");
        
        // blink on newly created item
        $(".collection-list li").first().addClass("blink-style");
    }

    private update(dto: Raven.Client.ServerWide.ETL.Transformation, isNew: boolean) {
        this.name(dto.Name);
        this.script(dto.Script);
        this.transformScriptCollections(dto.Collections || []);
        this.applyScriptForAllCollections(dto.ApplyToAllDocuments);
        this.isNew(isNew);
    }
}

export = ongoingTaskEtlTransformationModel;
