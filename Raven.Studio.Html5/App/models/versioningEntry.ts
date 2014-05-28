import documentMetadata = require("models/documentMetadata");

class versioningEntry {
    collection = ko.observable<string>();
    maxRevisions = ko.observable<number>();
    exclude = ko.observable<boolean>();
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
                Exclude: false
            };
            this.__metadata = new documentMetadata();
        } else {
            this.__metadata = new documentMetadata(dto["@metadata"]);
        }

        this.fromDatabase = ko.observable<boolean>(fromDatabse);
        this.collection = ko.observable<string>(dto.Id);
        this.maxRevisions = ko.observable<number>(dto.MaxRevisions);
        this.exclude = ko.observable<boolean>(dto.Exclude);
        this.removable = ko.computed<boolean>(() => {
            return (this.collection() !== "DefaultConfiguration");
        });

        this.isValid = ko.computed(() => {
            return this.collection() != null && this.collection().length > 0 && this.collection().indexOf(' ') === -1;
        });

        this.disabled = ko.computed(() => {
            return this.exclude();
        });

    }

    makeExcluded() {
        this.exclude(true);
    }

    makeIncluded() {
        this.exclude(false);
    }

    toDto(includeMetadata:boolean): versioningEntryDto {
        var dto = {
            '@metadata': undefined,
            Id: this.collection(),
            MaxRevisions: this.maxRevisions(),
            Exclude: this.exclude()
        };

        if (includeMetadata && this.__metadata) {
            dto['@metadata'] = this.__metadata.toDto();
        }

        return dto;
    }
}

export = versioningEntry;
