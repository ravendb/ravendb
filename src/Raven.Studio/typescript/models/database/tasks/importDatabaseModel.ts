class importDatabaseModel {
    includeDocuments = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeTransformers = ko.observable(true);
    removeAnalyzers = ko.observable(false);

    includeRevisionDocuments = ko.observable(true);
    includeExpiredDocuments = ko.observable(true);
    stripReplicationInformation = ko.observable(false);
    shouldDisableVersioningBundle = ko.observable(false);
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
        if (this.includeRevisionDocuments()) {
            operateOnTypes.push("RevisionDocuments");
        }

        //TODO: shouldDisableVersioningBundle - should we include this?
        return {
            IncludeExpired: this.includeExpiredDocuments(),
            TransformScript: this.transformScript(),
            RemoveAnalyzers: this.removeAnalyzers(),
            RemoveReplicationInformation: this.stripReplicationInformation(),
            OperateOnTypes: operateOnTypes.join(",") as Raven.Client.Smuggler.DatabaseItemType
        } as Raven.Client.Smuggler.DatabaseSmugglerOptions;
    }
}

export = importDatabaseModel;