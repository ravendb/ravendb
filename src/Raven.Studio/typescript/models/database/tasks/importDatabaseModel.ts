/// <reference path="../../../../typings/tsd.d.ts"/>

class importDatabaseModel {
    includeDocuments = ko.observable(true);
    includeIndexes = ko.observable(true);
    includeIdentities = ko.observable(true);
    removeAnalyzers = ko.observable(false);

    includeRevisionDocuments = ko.observable(true);
    includeExpiredDocuments = ko.observable(true);
    
    transformScript = ko.observable<string>();

    toDto(): Raven.Client.Documents.Smuggler.DatabaseSmugglerOptions {
        const operateOnTypes: Array<Raven.Client.Documents.Smuggler.DatabaseItemType> = [];
        if (this.includeDocuments()) {
            operateOnTypes.push("Documents");
        }
        if (this.includeIndexes()) {
            operateOnTypes.push("Indexes");
        }
        if (this.includeRevisionDocuments()) {
            operateOnTypes.push("RevisionDocuments");
        }
        if (this.includeIdentities()){
            operateOnTypes.push("Identities");
        }

        return {
            IncludeExpired: this.includeExpiredDocuments(),
            TransformScript: this.transformScript(),
            RemoveAnalyzers: this.removeAnalyzers(),
            OperateOnTypes: operateOnTypes.join(",") as Raven.Client.Documents.Smuggler.DatabaseItemType
        } as Raven.Client.Documents.Smuggler.DatabaseSmugglerOptions;
    }

}

export = importDatabaseModel;
