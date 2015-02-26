class versioningEntry {
    maxRevisions = ko.observable<number>();
    exclude = ko.observable<boolean>();
    excludeUnlessExplicit = ko.observable<boolean>();
    purgeOnDelete = ko.observable<boolean>();

    disabled: KnockoutComputed<boolean>;

    constructor(dto?: versioningEntryDto) {
        if (!dto) {
            // Default settings for new entries
            dto = {
                Id: dto.Id,
                MaxRevisions: 214748368,
                Exclude: false,
                ExcludeUnlessExplicit: false,
                PurgeOnDelete: false
            };
        }

        this.maxRevisions(dto.MaxRevisions);
        this.exclude(dto.Exclude);
        this.excludeUnlessExplicit(dto.ExcludeUnlessExplicit);
        this.purgeOnDelete(dto.PurgeOnDelete);
        this.disabled = ko.computed(() => this.exclude());
        
    }

    makeExcluded() {
        this.exclude(true);
    }

    makeIncluded() {
        this.exclude(false);
    }

    toDto(): versioningEntryDto {
        var dto = {
            Id: 'Raven/Versioning/DefaultConfiguration',
            MaxRevisions: this.maxRevisions(),
            Exclude: this.exclude(),
            ExcludeUnlessExplicit: this.excludeUnlessExplicit(),
            PurgeOnDelete: this.purgeOnDelete()
        };

        return dto;
    }
}

export = versioningEntry;
