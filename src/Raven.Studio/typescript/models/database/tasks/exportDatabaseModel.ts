class exportDatabaseModel {

    includeDocuments = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeTransformers = ko.observable(true);
    includeIdentities = ko.observable(true);
    includeRevisionDocuments = ko.observable(true);
    
    exportFileName = ko.observable<string>();

    batchSize = ko.observable(1024);
    includeExpiredDocuments = ko.observable(false);
    removeAnalyzers = ko.observable(false);

    includeAllCollections = ko.observable(true);
    includedCollections = ko.observableArray<string>([]);

    transformScript = ko.observable<string>();


    toDto(): Raven.Client.Smuggler.DatabaseSmugglerOptions {
        const operateOnTypes: Array<Raven.Client.Smuggler.DatabaseItemType> = [];
        if (this.includeDocuments()) {
            operateOnTypes.push("Documents");
        }
        if (this.includeIndexes()) {
            operateOnTypes.push("Indexes");
        }
        if (this.includeTransformers()) {
            operateOnTypes.push("Transformers");
        }
        if (this.includeIdentities()) {
            operateOnTypes.push("Identities");
        }
        if (this.includeRevisionDocuments()) {
            operateOnTypes.push("RevisionDocuments");
        }

        return {
            BatchSize: this.batchSize(),
            CollectionsToExport: this.includeAllCollections() ? null : this.includedCollections(),
            FileName: this.exportFileName(),
            IncludeExpired: this.includeExpiredDocuments(),
            TransformScript: this.transformScript(),
            RemoveAnalyzers: this.removeAnalyzers(),
            OperateOnTypes: operateOnTypes.join(",") as Raven.Client.Smuggler.DatabaseItemType
        } as Raven.Client.Smuggler.DatabaseSmugglerOptions;
    }
}

export = exportDatabaseModel;