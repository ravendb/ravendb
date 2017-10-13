/// <reference path="../../../../typings/tsd.d.ts"/>
import collectionsTracker = require("common/helpers/database/collectionsTracker");

class ongoingTaskEtlTransformationModel {
    name = ko.observable<string>();
    script = ko.observable<string>();
    transformScriptCollections = ko.observableArray<string>([]);

    applyScriptForAllCollections = ko.observable<boolean>(false);
    isNew = ko.observable<boolean>(true);
    inputCollection = ko.observable<string>();
    canAddCollection: KnockoutComputed<boolean>;

    validationGroup: KnockoutValidationGroup; 
  
    constructor(dto: Raven.Client.ServerWide.ETL.Transformation, isNew: boolean) {
        this.update(dto, isNew);
        this.initObservables();
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
        this.transformScriptCollections.push(this.inputCollection());
        this.inputCollection("");
    }
    
    autoAddCollection(collectionName: string) {
        this.transformScriptCollections.unshift(collectionName);
        this.inputCollection("");
        
        // blink on newly created item
        $(".collection-list li").first().addClass("blink-style");
    }

    update(dto: Raven.Client.ServerWide.ETL.Transformation, isNew: boolean) {
        this.name(dto.Name);
        this.script(dto.Script);
        this.transformScriptCollections(dto.Collections || []);
        this.applyScriptForAllCollections(dto.ApplyToAllDocuments);
        this.isNew(isNew);

        // Reset validation for this transformation script model 
        this.name.extend({ validatable: false });
        this.transformScriptCollections.extend({ validatable: false });
        this.initValidation();
    }
}

export = ongoingTaskEtlTransformationModel;
