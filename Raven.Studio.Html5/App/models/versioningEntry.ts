import documentMetadata = require("models/documentMetadata");

class versioningEntry implements copyFromParentDto<versioningEntry> {
    collection = ko.observable<string>().extend({ required: true });
    maxRevisions = ko.observable<number>().extend({ required: true });
    exclude = ko.observable<boolean>().extend({ required: true });

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

        this.fromDatabase(fromDatabse);
        this.collection(dto.Id);
        this.maxRevisions(dto.MaxRevisions);
        this.exclude(dto.Exclude);
        this.removable = ko.computed<boolean>(() => {
            return (this.collection() !== "DefaultConfiguration");
        });

        this.isValid = ko.computed(() => {
            return this.collection() != null && this.collection().length > 0 && this.collection().indexOf(' ') === -1;
        });

        this.disabled = ko.computed(() => this.exclude());
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
            dto["@metadata"] = <any>(this.__metadata.toDto());
        }

        return dto;
    }

    copyFromParent(parent: versioningEntry) {
        this.collection(parent.collection());
        this.maxRevisions(parent.maxRevisions());
        this.exclude(parent.exclude());
        this.fromDatabase(true);
        this.__metadata = parent.__metadata;
    }
}

export = versioningEntry;
