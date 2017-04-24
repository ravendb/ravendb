/// <reference path="../../../../typings/tsd.d.ts" />

type knownDocumentFlags = "Versioned" | "Revision" | "HasAttachments";

class documentMetadata {
    collection: string;
    ravenClrType: string;
    nonAuthoritativeInfo: boolean;
    id: string;
    tempIndexScore: number;
    flags: string;
    lastModified = ko.observable<string>();
    nonStandardProps: Array<keyof documentMetadataDto>;

    etag = ko.observable<number>(null);
    lastModifiedFullDate: KnockoutComputed<string>;

    attachments: Array<documentAttachmentDto>;
    changeVector: Array<changeVectorDto>;

    constructor(dto?: documentMetadataDto) {
        if (dto) {
            this.collection = dto['@collection'];
            this.flags = dto['@flags'];
            this.ravenClrType = dto['Raven-Clr-Type'];
            this.nonAuthoritativeInfo = dto['Non-Authoritative-Information'];
            this.id = dto['@id'];
            this.tempIndexScore = dto['Temp-Index-Score'];
            this.lastModifiedFullDate = ko.pureComputed(() => {
                if (this.lastModified()) {
                    const lastModifiedMoment = moment(this.lastModified());
                    return lastModifiedMoment.utc().format("DD/MM/YYYY HH:mm (UTC)");
                }
                return "";
            });
            this.lastModified(dto['@last-modified']);

            this.etag(dto['@etag']);

            this.attachments = dto['@attachments'];

            this.changeVector = dto['@change-vector'];
            
            for (let property in dto) {
                if (property.toUpperCase() !== '@collection'.toUpperCase() &&
                    property.toUpperCase() !== '@flags'.toUpperCase() &&
                    property.toUpperCase() !== 'Raven-Clr-Type'.toUpperCase() &&
                    property.toUpperCase() !== 'Non-Authoritative-Information'.toUpperCase() &&
                    property.toUpperCase() !== '@id'.toUpperCase() &&
                    property.toUpperCase() !== 'Temp-Index-Score'.toUpperCase() &&
                    property.toUpperCase() !== '@last-modified'.toUpperCase() &&
                    property.toUpperCase() !== '@attachments'.toUpperCase() &&
                    property.toUpperCase() !== '@etag'.toUpperCase() &&
                    property.toUpperCase() !== 'toDto'.toUpperCase() &&
                    property.toUpperCase() !=='@change-vector'.toUpperCase()) {
                    this.nonStandardProps = this.nonStandardProps || [];
                    (<any>this)[property] = (<any>dto)[property];
                    this.nonStandardProps.push(property as any);
                }
            }
        }
    }

    hasFlag(flag: knownDocumentFlags) {
        return _.includes(this.flags, flag);
    }

    toDto(): documentMetadataDto {
        const dto: documentMetadataDto = {
            '@collection': this.collection,
            'Raven-Clr-Type': this.ravenClrType,
            '@flags': this.flags,
            'Non-Authoritative-Information': this.nonAuthoritativeInfo,
            '@id': this.id,
            'Temp-Index-Score': this.tempIndexScore,
            '@last-modified': this.lastModified(),
            '@etag': this.etag(),
            '@attachments': this.attachments,
            '@change-vector': this.changeVector
    };

        if (this.nonStandardProps) {
            this.nonStandardProps.forEach(p => dto[p] = (<any>this)[p]);
        }

        return dto;
    }

    static filterMetadata(metaDto: documentMetadataDto, removedProps: any[] = null, isClonedDocument: boolean = false) {
        // We don't want to show certain reserved properties in the metadata text area.
        // Remove them from the DTO, restore them on save.
        const metaPropsToRemove = ["@id", "@etag", "@last-modified", "@attachments"];

        if (isClonedDocument) {
            metaPropsToRemove.push("@change-vector");
        }

        for (let property in metaDto) {
            if (metaDto.hasOwnProperty(property) && _.includes(metaPropsToRemove, property)) {
                if ((<any>metaDto)[property] && removedProps) {
                    removedProps.push({ name: property, value: (<any>metaDto)[property] });
                }
                delete (<any>metaDto)[property];
            }
        }
        return metaDto;
    }
}

export = documentMetadata;
