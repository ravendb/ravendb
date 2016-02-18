/// <reference path="../../../typings/tsd.d.ts"/>

class synchronizationDetail implements documentBase {

    fileName = ko.observable<string>();
    DestinationUrl = ko.observable<string>();
    Type = ko.observable<filesystemSynchronizationType>();
    TypeDescription = ko.observable<string>();
    Status = ko.observable<string>();
    Direction = ko.observable<synchronizationDirection>();

    constructor(dto?: synchronizationUpdateNotification | filesystemSynchronizationDetailsDto, status?: string, type?: string, destinationUrl?: string) {

        this.fileName(dto.FileName);

        this.DestinationUrl(destinationUrl);
        if (type) {
            this.Type(synchronizationDetail.getType(type));
        }
        else {
            this.Type((<any>dto).Type);
        }
        this.TypeDescription(synchronizationDetail.getTypeDescription(this.Type()));
        this.Status(status);
        this.Direction(dto.Direction);
    }

    getId() {
        return this.fileName();
    }

    getUrl() {
        return this.getId();
    }

    getDocumentPropertyNames(): Array<string> {
        return ["Id", "DestinationUrl", "Type", "Status"];
    }

    static getType(typeAsString: string) {
        switch (typeAsString) {
            case "ContentUpdate":
                return filesystemSynchronizationType.ContentUpdate;
            case "Delete":
                return filesystemSynchronizationType.Delete;
            case "MetadataUpdate":
                return filesystemSynchronizationType.MetadataUpdate;
            case "Rename":
                return filesystemSynchronizationType.Rename;
            default:
                return filesystemSynchronizationType.Unknown;
        }
    }

    static getTypeDescription(type: filesystemSynchronizationType) {
        switch (type) {
            case filesystemSynchronizationType.ContentUpdate:
                return "Content Update";
            case filesystemSynchronizationType.Delete:
                return "Delete";
            case filesystemSynchronizationType.MetadataUpdate:
                return "Metadata Update";
            case filesystemSynchronizationType.Rename:
                return "Rename";
            default:
                return "Unknown";
        }
    }
}

export = synchronizationDetail;

