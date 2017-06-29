/// <reference path="../../../../typings/tsd.d.ts"/>

class revisionsConfigurationEntry {

    static readonly DefaultConfiguration = "DefaultConfiguration";

    disabled = ko.observable<boolean>();
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
        this.disabled(!dto.Active);
        this.purgeOnDelete(dto.PurgeOnDelete);
        this.isDefault = ko.pureComputed<boolean>(() => this.collection() === revisionsConfigurationEntry.DefaultConfiguration);

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
            Active: !this.disabled(),
            MinimumRevisionsToKeep: this.minimumRevisionsToKeep(),
            MinimumRevisionAgeToKeep: this.minimumRevisionAgeToKeep(),
            PurgeOnDelete: this.purgeOnDelete()
        };
    }

    static empty() {
        return new revisionsConfigurationEntry("",
        {
            Active: true,
            MinimumRevisionsToKeep: null,
            MinimumRevisionAgeToKeep: null,
            PurgeOnDelete: false
        });
    }

    static defaultConfiguration() {
        const item = revisionsConfigurationEntry.empty();
        item.collection(revisionsConfigurationEntry.DefaultConfiguration);
        return item;
    }
}

export = revisionsConfigurationEntry;
