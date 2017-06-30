/// <reference path="../../../../typings/tsd.d.ts"/>

class revisionsConfigurationEntry {

    static readonly DefaultConfiguration = "DefaultConfiguration";

    disabled = ko.observable<boolean>();
    purgeOnDelete = ko.observable<boolean>();
    collection = ko.observable<string>();

    limitRevisions = ko.observable<boolean>();
    minimumRevisionsToKeep = ko.observable<number>();

    limitRevisionsByAge = ko.observable<boolean>(false);
    minimumRevisionAgeToKeep = ko.observable<string>();

    isDefault: KnockoutComputed<boolean>;
    humaneRetentionDescription: KnockoutComputed<string>;

    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        collection: this.collection,
        minimumRevisionsToKeep: this.minimumRevisionsToKeep,
        minimumRevisionAgeToKeep: this.minimumRevisionAgeToKeep
    });

    constructor(collection: string, dto: Raven.Client.Server.Versioning.VersioningCollectionConfiguration) {
        this.collection(collection);

        this.limitRevisions(dto.MinimumRevisionsToKeep != null);
        this.minimumRevisionsToKeep(dto.MinimumRevisionsToKeep);

        this.limitRevisionsByAge(dto.MinimumRevisionAgeToKeep != null);
        this.minimumRevisionAgeToKeep(dto.MinimumRevisionAgeToKeep);

        this.disabled(!dto.Active);
        this.purgeOnDelete(dto.PurgeOnDelete);
        this.isDefault = ko.pureComputed<boolean>(() => this.collection() === revisionsConfigurationEntry.DefaultConfiguration);

        this.initObservables();
        this.initValidation();
    }
    private initObservables() {
        this.humaneRetentionDescription = ko.pureComputed(() => {
            const retentionTimeHumane = this.minimumRevisionAgeToKeep(); //TODO: format me!
            const agePart = this.limitRevisionsByAge() && this.minimumRevisionAgeToKeep.isValid() ? `Revisions are going to be removed on next revision creation once they exceed retention time of <strong>${retentionTimeHumane}</strong>. ` : "";
            const countPart = this.limitRevisions() && this.minimumRevisionsToKeep.isValid() ? `At least <strong>${this.minimumRevisionsToKeep()}</strong> revisions are going to be kept. ` : "";
            return agePart + countPart;
        });

        this.limitRevisions.subscribe(v => {
            this.minimumRevisionsToKeep.clearError();
        });

        this.limitRevisionsByAge.subscribe(v => {
            this.minimumRevisionAgeToKeep.clearError();
        });
    }

    private initValidation() {
        this.collection.extend({
            required: true
        });

        this.minimumRevisionsToKeep.extend({
            required: {
                onlyIf: () => this.limitRevisions()
            },
            min: 0
        });

        this.minimumRevisionAgeToKeep.extend({
            required: {
                onlyIf: () => this.limitRevisionsByAge()
            }
        });
    }

    copyFrom(incoming: revisionsConfigurationEntry): this {
        this.disabled(incoming.disabled());
        this.purgeOnDelete(incoming.purgeOnDelete());
        this.collection(incoming.collection());

        this.limitRevisions(incoming.minimumRevisionsToKeep() != null);
        this.minimumRevisionsToKeep(incoming.minimumRevisionsToKeep());

        this.limitRevisionsByAge(incoming.minimumRevisionAgeToKeep() != null);
        this.minimumRevisionAgeToKeep(incoming.minimumRevisionAgeToKeep());
        
        return this;
    }

    toDto(): Raven.Client.Server.Versioning.VersioningCollectionConfiguration {
        return {
            Active: !this.disabled(),
            MinimumRevisionsToKeep: this.limitRevisions() ? this.minimumRevisionsToKeep() : null,
            MinimumRevisionAgeToKeep: this.limitRevisionsByAge() ? this.minimumRevisionAgeToKeep() : null,
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
