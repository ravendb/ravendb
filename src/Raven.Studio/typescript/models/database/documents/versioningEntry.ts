/// <reference path="../../../../typings/tsd.d.ts"/>

class versioningEntry {

    static readonly DefaultConfiguration = "DefaultConfiguration";

    active = ko.observable<boolean>();
    maxRevisions = ko.observable<number>();
    purgeOnDelete = ko.observable<boolean>();
    collection = ko.observable<string>().extend({ required: true});

    /*TODO
    exclude = ko.observable<boolean>();
    excludeUnlessExplicit = ko.observable<boolean>();*/

    isDefault: KnockoutComputed<boolean>;

    constructor(collection: string, dto: Raven.Server.Documents.Versioning.VersioningConfigurationCollection) {
        this.collection(collection);
        this.maxRevisions(dto.MaxRevisions);
        this.active(dto.Active);
        this.purgeOnDelete(dto.PurgeOnDelete);
        this.isDefault = ko.pureComputed<boolean>(() => this.collection() === versioningEntry.DefaultConfiguration);
    }

    toDto(): Raven.Server.Documents.Versioning.VersioningConfigurationCollection {
        return {
            Active: this.active(),
            MaxRevisions: this.maxRevisions(),
            PurgeOnDelete: this.purgeOnDelete()
        };
    }

    static empty() {
        return new versioningEntry("",
        {
            Active: true,
            MaxRevisions: 5,
            PurgeOnDelete: false
        });
    }
}

export = versioningEntry;
