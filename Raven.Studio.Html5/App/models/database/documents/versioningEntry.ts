import documentMetadata = require("models/database/documents/documentMetadata");

class versioningEntry implements copyFromParentDto<versioningEntry> {
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

        var id = dto.Id;
        if (!id && this.__metadata.id) {
            // pre 3.0 backward compatibility
            var prefix = "Raven/Versioning/";
            id = this.__metadata.id;
            if (id && id.startsWith(prefix)) {
                id = id.substring(prefix.length);
            } 
        }
        
        this.collection(id);
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
            dto["@metadata"] = <any>(this.__metadata.toDto());
        }

        return dto;
    }

    copyFromParent(parent: versioningEntry) {
        this.collection(parent.collection());
        this.maxRevisions(parent.maxRevisions());
        this.exclude(parent.exclude());
        this.excludeUnlessExplicit(parent.excludeUnlessExplicit());
        this.purgeOnDelete(parent.purgeOnDelete());
        this.fromDatabase(true);
        this.__metadata = parent.__metadata;
    }
}

export = versioningEntry;
