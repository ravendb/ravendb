class importDatabaseModel {
    includeDocuments = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeTransformers = ko.observable(true);
    removeAnalyzers = ko.observable(false);

    batchSize = ko.observable(1024);
    includeExpiredDocuments = ko.observable(true);
    stripReplicationInformation = ko.observable(false);
    shouldDisableVersioningBundle = ko.observable(false);
    transformScript = ko.observable<string>();


    toDto(): importDatabaseRequestDto {
        return {
            batchSize: this.batchSize(),

            //TODO: send other props
        }
    }
}

export = importDatabaseModel;