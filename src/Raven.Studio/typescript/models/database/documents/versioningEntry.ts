/// <reference path="../../../../typings/tsd.d.ts"/>

class versioningEntry {

    static readonly DefaultConfiguration = "DefaultConfiguration";

    active = ko.observable<boolean>();
    minimumRevisionsToKeep = ko.observable<number>();
    minimumRevisionAgeToKeep = ko.observable<string>();
    purgeOnDelete = ko.observable<boolean>();
    collection = ko.observable<string>();

    isDefault: KnockoutComputed<boolean>;

    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        collection: this.collection,
        minimumRevisionsToKeep: this.minimumRevisionsToKeep,
        minimumRevisionAgeToKeep: this.minimumRevisionAgeToKeep,
    });

    constructor(collection: string, dto: Raven.Client.Server.Versioning.VersioningCollectionConfiguration) {
        this.collection(collection);
        this.minimumRevisionsToKeep(dto.MinimumRevisionsToKeep);
        this.minimumRevisionAgeToKeep(dto.MinimumRevisionAgeToKeep);
        this.active(dto.Active);
        this.purgeOnDelete(dto.PurgeOnDelete);
        this.isDefault = ko.pureComputed<boolean>(() => this.collection() === versioningEntry.DefaultConfiguration);

        this.initValidation();
    }

    private initValidation() {
        this.collection.extend({
            required: true
        });

        this.minimumRevisionsToKeep.extend({
            min: 0
        });

        //TODO: this.minimumRevisionAgeToKeep.extend({
        //});
    }

    toDto(): Raven.Client.Server.Versioning.VersioningCollectionConfiguration {
        return {
            Active: this.active(),
            MinimumRevisionsToKeep: this.minimumRevisionsToKeep(),
            MinimumRevisionAgeToKeep: this.minimumRevisionAgeToKeep(),
            PurgeOnDelete: this.purgeOnDelete()
        };
    }

    static empty() {
        return new versioningEntry("",
        {
            Active: true,
            MinimumRevisionsToKeep: null,
            MinimumRevisionAgeToKeep: null,
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
