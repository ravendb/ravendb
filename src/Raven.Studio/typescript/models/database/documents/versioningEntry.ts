/// <reference path="../../../../typings/tsd.d.ts"/>

class versioningEntry {

    static readonly DefaultConfiguration = "DefaultConfiguration";

    active = ko.observable<boolean>();
    maxRevisions = ko.observable<number>();
    purgeOnDelete = ko.observable<boolean>();
    collection = ko.observable<string>();

    isDefault: KnockoutComputed<boolean>;

    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        collection: this.collection,
        maxRevisions: this.maxRevisions
    });

    constructor(collection: string, dto: Raven.Client.Server.Versioning.VersioningConfigurationCollection) {
        this.collection(collection);
        this.maxRevisions(dto.MaxRevisions);
        this.active(dto.Active);
        this.purgeOnDelete(dto.PurgeOnDelete);
        this.isDefault = ko.pureComputed<boolean>(() => this.collection() === versioningEntry.DefaultConfiguration);

        this.initValidation();
    }

    private initValidation() {
        this.collection.extend({
            required: true
        });

        this.maxRevisions.extend({
            min: 0
        });
    }

    toDto(): Raven.Client.Server.Versioning.VersioningConfigurationCollection {
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

    static defaultConfiguration() {
        const item = versioningEntry.empty();
        item.collection(versioningEntry.DefaultConfiguration);
        return item;
    }
}

export = versioningEntry;
