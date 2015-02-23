import documentMetadata = require("models/documentMetadata");

class versioningEntry {
    collection = ko.observable<string>();
    maxRevisions = ko.observable<number>();
    exclude = ko.observable<boolean>();
    excludeUnlessExplicit = ko.observable<boolean>();
    purgeOnDelete = ko.observable<boolean>();

    fromDatabase = ko.observable<boolean>();

    removable: KnockoutComputed<boolean>;
    isValid: KnockoutComputed<boolean>;
    disabled: KnockoutComputed<boolean>;
    __metadata: documentMetadata;

    constructor(dto?: versioningEntryDto, fromDatabse: boolean = false) {
        if (!dto) {
            // Default settings for new entries
            dto = {
                Id: "",
                MaxRevisions: 214748368,
                Exclude: false,
                ExcludeUnlessExplicit: false,
                PurgeOnDelete: false
            };
            this.__metadata = new documentMetadata();
        } else {
            this.__metadata = new documentMetadata(dto["@metadata"]);
        }

        this.fromDatabase(fromDatabse);
        this.collection(dto.Id);
        this.maxRevisions(dto.MaxRevisions);
        this.exclude(dto.Exclude);
        this.excludeUnlessExplicit(dto.ExcludeUnlessExplicit);
        this.purgeOnDelete(dto.PurgeOnDelete);
        this.removable = ko.computed<boolean>(() => {
            return (this.collection() !== "DefaultConfiguration");
        });

        this.isValid = ko.computed(() => {
            return this.collection() != null && this.collection().length > 0 && this.collection().indexOf(' ') === -1;
        });

        this.disabled = ko.computed(() => this.exclude());
    }

    toDto(includeMetadata:boolean): versioningEntryDto {
        var dto: versioningEntryDto = {
            '@metadata': undefined,
            Id: this.collection(),
            MaxRevisions: this.maxRevisions(),
            Exclude: this.exclude(),
            ExcludeUnlessExplicit: this.excludeUnlessExplicit(),
            PurgeOnDelete: this.purgeOnDelete()
        };

        if (includeMetadata && this.__metadata) {
            dto['@metadata'] = this.__metadata.toDto();
        }

        return dto;
    }
}

export = versioningEntry;
