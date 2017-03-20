/// <reference path="../../../../typings/tsd.d.ts" />

class documentMetadata {
    collection: string;
    ravenClrType: string;
    nonAuthoritativeInfo: boolean;
    id: string;
    tempIndexScore: number;
    lastModified = ko.observable<string>();
    nonStandardProps: Array<keyof documentMetadataDto>;
   
    etag = ko.observable<number>(null);
    lastModifiedFullDate: KnockoutComputed<string>;

    constructor(dto?: documentMetadataDto) {
        if (dto) {
            this.collection = dto['@collection'];
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

            for (let property in dto) {
                if (property.toUpperCase() !== '@collection'.toUpperCase() &&
                    property.toUpperCase() !== 'Raven-Clr-Type'.toUpperCase() &&
                    property.toUpperCase() !== 'Non-Authoritative-Information'.toUpperCase() &&
                    property.toUpperCase() !== '@id'.toUpperCase() &&
                    property.toUpperCase() !== 'Temp-Index-Score'.toUpperCase() &&
                    property.toUpperCase() !== '@last-modified'.toUpperCase() &&
                    property.toUpperCase() !== '@etag'.toUpperCase() &&
                    property.toUpperCase() !== 'toDto'.toUpperCase()) {
                    this.nonStandardProps = this.nonStandardProps || [];
                    (<any>this)[property] = (<any>dto)[property];
                    this.nonStandardProps.push(property as any);
                }
            }
        }
    }

    toDto(): documentMetadataDto {
        const dto: documentMetadataDto = {
            '@collection': this.collection,
            'Raven-Clr-Type': this.ravenClrType,
            'Non-Authoritative-Information': this.nonAuthoritativeInfo,
            '@id': this.id,
            'Temp-Index-Score': this.tempIndexScore,
            '@last-modified': this.lastModified(),
            '@etag': this.etag()
        };

        if (this.nonStandardProps) {
            this.nonStandardProps.forEach(p => dto[p] = (<any>this)[p]);
        }

        return dto;
    }


    static filterMetadata(metaDto: documentMetadataDto, removedProps: any[] = null) {
        // We don't want to show certain reserved properties in the metadata text area.
        // Remove them from the DTO, restore them on save.
        const metaPropsToRemove = ["@id", "@etag", "@last-modified"];

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
