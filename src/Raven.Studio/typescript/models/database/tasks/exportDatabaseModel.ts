class exportDatabaseModel {

    includeDocuments = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeTransformers = ko.observable(true);
    removeAnalyzers = ko.observable(false);
    //TODO: Identities

    exportFileName = ko.observable<string>();

    batchSize = ko.observable(1024);
    includeExpiredDocuments = ko.observable(false);

    includeAllCollections = ko.observable(true);
    includedCollections = ko.observableArray<string>([]);

    transformScript = ko.observable<string>();


    toDto(): smugglerOptionsDto {
        let operateOnTypes = 0;
        if (this.includeDocuments()) {
            operateOnTypes += 1;
        }
        if (this.includeIndexes()) {
            operateOnTypes += 2;
        }
        if (this.includeTransformers()) {
            operateOnTypes += 8;
        }
        if (this.removeAnalyzers()) {
            operateOnTypes += 8000;
        }

        const filtersToSend: filterSettingDto[] = [];

        if (!this.includeAllCollections()) {
            filtersToSend.push(
                {
                    ShouldMatch: true,
                    Path: "@metadata.Raven-Entity-Name",
                    Values: this.includedCollections()
                });
        }

        return {
            OperateOnTypes: operateOnTypes,
            BatchSize: this.batchSize(),
            ShouldExcludeExpired: !this.includeExpiredDocuments(),
            Filters: filtersToSend,
            TransformScript: this.transformScript(),
            NoneDefaultFileName: this.exportFileName()
        } as smugglerOptionsDto;
    }
}

export = exportDatabaseModel;