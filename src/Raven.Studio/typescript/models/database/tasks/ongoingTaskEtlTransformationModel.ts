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
        return (_.findIndex(collectionsTracker.default.collections(), x => x.name === collectionName) - 1) % 6;
        // 6 is the number of classes that I have defined in etl.less for colors...
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
            Collections: this.transformScriptCollections(),
            Disabled: false,
            HasLoadAttachment: false,
            Name: this.name(),
            Script: this.script()
        }
    }

    private initObservables() {
        this.applyScriptForAllCollections.subscribe(x => {
            if (x) {
                // Add all collections to the 'used' collections...
                this.transformScriptCollections(collectionsTracker.default.collections()
                    .filter(x => !x.isAllDocuments)
                    .map(x => x.name));
            }
            else {
                this.transformScriptCollections([]);
            }
        });

        this.canAddCollection = ko.pureComputed(() => {
            // Add collection only if exists in collection tracker & not yet added...
            return !!collectionsTracker.default.collections().find(x => x.name === this.inputCollection()) &&
                   !this.transformScriptCollections().find(x => x === this.inputCollection());
        }).extend({ throttle: 200});
    }

    private initValidation() {
        this.name.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            name: this.name
        });
    }

    removeCollection(collection: string) {
        this.transformScriptCollections.remove(collection);
        this.applyScriptForAllCollections(false);
    }

    addCollection() {
        this.transformScriptCollections.push(this.inputCollection());
        this.inputCollection("");
        this.transformScriptCollections.sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));
    }

    update(dto: Raven.Client.ServerWide.ETL.Transformation, isNew: boolean) {
        this.name(dto.Name);
        this.script(dto.Script);
        this.transformScriptCollections(dto.ApplyToAllDocuments ? collectionsTracker.default.collections()
            .filter(x => !x.isAllDocuments)
            .map(x => x.name) : dto.Collections);
        this.applyScriptForAllCollections(dto.ApplyToAllDocuments);
        this.isNew(isNew);

        // Reset validation for this transformation script model 
        this.name.extend({ validatable: false });
        this.transformScriptCollections.extend({ validatable: false });
        this.initValidation();
    }
}

export = ongoingTaskEtlTransformationModel;
