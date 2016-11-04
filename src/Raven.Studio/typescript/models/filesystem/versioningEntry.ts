/// <reference path="../../../typings/tsd.d.ts"/>

class versioningEntry {
    /* TODO
    maxRevisions = ko.observable<number>();
    exclude = ko.observable<boolean>();
    excludeUnlessExplicit = ko.observable<boolean>();
    purgeOnDelete = ko.observable<boolean>();
    resetOnRename = ko.observable<boolean>();

    disabled: KnockoutComputed<boolean>;

    constructor(dto?: versioningEntryDto) {
        if (!dto) {
            // Default settings for new entries
            dto = {
                Id: dto.Id,
                MaxRevisions: 214748368,
                Exclude: false,
                ExcludeUnlessExplicit: false,
                PurgeOnDelete: false,
                ResetOnRename: false
            };
        }

        this.maxRevisions(dto.MaxRevisions);
        this.exclude(dto.Exclude);
        this.excludeUnlessExplicit(dto.ExcludeUnlessExplicit);
        this.purgeOnDelete(dto.PurgeOnDelete);
        this.resetOnRename(dto.ResetOnRename);
        this.disabled = ko.computed(() => this.exclude());
        
    }

    toDto(): versioningEntryDto {
        var dto = {
            Id: 'Raven/Versioning/DefaultConfiguration',
            MaxRevisions: this.maxRevisions(),
            Exclude: this.exclude(),
            ExcludeUnlessExplicit: this.excludeUnlessExplicit(),
            PurgeOnDelete: this.purgeOnDelete(),
            ResetOnRename: this.resetOnRename()
        };

        return dto;
    }*/
}

export = versioningEntry;
