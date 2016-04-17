class synchronizationDetail implements documentBase {

    FileName = ko.observable<string>();
    FileEtag = ko.observable<string>();
    DestinationUrl = ko.observable<string>();
    Type = ko.observable<synchronizationType>();
    TypeDescription = ko.observable<string>();
    Status = ko.observable<synchronizationActivity>();
    Direction = ko.observable<synchronizationDirection>();
    AdditionalInfo = ko.observable<any>();

    constructor(dto: synchronizationDetailsDto, direction: synchronizationDirection, status?: synchronizationActivity) {

        this.FileName(dto.FileName);
        this.FileEtag(dto.FileETag);
        this.DestinationUrl(dto.DestinationUrl);
        this.Type(synchronizationDetail.getType(dto.Type));
        this.TypeDescription(synchronizationDetail.getTypeDescription(this.Type()));
        this.Status(status);
        this.Direction(direction);
    }

    getId() {
        return this.FileName();
    }

    getUrl() {
        return this.getId();
    }

    getDocumentPropertyNames(): Array<string> {
        return ["Id", "FileEtag", "DestinationUrl", "Type", "Status"];
    }

    static getType(typeAsString: string) {
        switch (typeAsString) {
            case "ContentUpdate":
                return synchronizationType.ContentUpdate;
            case "Delete":
                return synchronizationType.Delete;
            case "MetadataUpdate":
                return synchronizationType.MetadataUpdate;
            case "Rename":
                return synchronizationType.Rename;
            default:
                return synchronizationType.Unknown;
        }
    }

    static getTypeDescription(type: synchronizationType) {
        switch (type) {
            case synchronizationType.ContentUpdate:
                return "Content Update";
            case synchronizationType.Delete:
                return "Delete";
            case synchronizationType.MetadataUpdate:
                return "Metadata Update";
            case synchronizationType.Rename:
                return "Rename";
            default:
                return "Unknown";
        }
    }
}

export = synchronizationDetail;

