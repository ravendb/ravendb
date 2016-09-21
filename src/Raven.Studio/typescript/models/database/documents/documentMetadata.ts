/// <reference path="../../../../typings/tsd.d.ts" />

class documentMetadata {
    ravenEntityName: string;
    ravenClrType: string;
    nonAuthoritativeInfo: boolean;
    id: string;
    tempIndexScore: number;
    lastModified: string;
    ravenLastModified: string;
    etag: number;
    nonStandardProps: Array<string>;

    lastModifiedFullDate: KnockoutComputed<string>;
    lastModifiedAsAgo: KnockoutComputed<string>;
    now = ko.observable(new Date());

    constructor(dto?: documentMetadataDto) {
        if (dto) {
            this.ravenEntityName = dto['Raven-Entity-Name'];
            this.ravenClrType = dto['Raven-Clr-Type'];
            this.nonAuthoritativeInfo = dto['Non-Authoritative-Information'];
            this.id = dto['@id'];
            this.tempIndexScore = dto['Temp-Index-Score'];
            this.lastModified = dto['Last-Modified'];

            setInterval(() => this.now(new Date()), 60*1000);

            this.ravenLastModified = dto['Raven-Last-Modified'];
            this.lastModifiedAsAgo = ko.computed(() => {
                if (!!this.ravenLastModified) {
                    const lastModifiedMoment = moment(this.ravenLastModified);
                    return lastModifiedMoment.from(this.now());
                }
                return "";
            });

            this.lastModifiedFullDate = ko.computed(() => {
                if (!!this.ravenLastModified) {
                    const lastModifiedMoment = moment(this.ravenLastModified);
                    const fullTimeSinceUtc = lastModifiedMoment.utc().format("DD/MM/YYYY HH:mm (UTC)");
                    return fullTimeSinceUtc;
                }
                return "";
            });
            this.etag = dto['@etag'];

            for (var property in dto) {
                if (property.toUpperCase() !== 'Raven-Entity-Name'.toUpperCase() &&
                    property.toUpperCase() !== 'Raven-Clr-Type'.toUpperCase() &&
                    property.toUpperCase() !== 'Non-Authoritative-Information'.toUpperCase() &&
                    property.toUpperCase() !== '@id'.toUpperCase() &&
                    property.toUpperCase() !== 'Temp-Index-Score'.toUpperCase() &&
                    property.toUpperCase() !== 'Last-Modified'.toUpperCase() &&
                    property.toUpperCase() !== 'Raven-Last-Modified'.toUpperCase() &&
                    property.toUpperCase() !== '@etag'.toUpperCase() &&
                    property.toUpperCase() !== 'toDto'.toUpperCase()) {
                    this.nonStandardProps = this.nonStandardProps || [];
                    (<any>this)[property] = (<any>dto)[property];
                    this.nonStandardProps.push(property);
                }
            }
        }
    }

    toDto(): documentMetadataDto {
        var dto: any = {
            'Raven-Entity-Name': this.ravenEntityName,
            'Raven-Clr-Type': this.ravenClrType,
            'Non-Authoritative-Information': this.nonAuthoritativeInfo,
            '@id': this.id,
            'Temp-Index-Score': this.tempIndexScore,
            'Last-Modified': this.lastModified,
            'Raven-Last-Modified': this.ravenLastModified,
            '@etag': this.etag
        };

        if (this.nonStandardProps) {
            this.nonStandardProps.forEach(p => dto[p] = (<any>this)[p]);
        }

        return dto;
    }


    static filterMetadata(metaDto: documentMetadataDto, removedProps: any[] = null) {
        // We don't want to show certain reserved properties in the metadata text area.
        // Remove them from the DTO, restore them on save.
        var metaPropsToRemove = ["@id", "@etag", "Raven-Last-Modified"];

        for (var property in metaDto) {
            if (metaDto.hasOwnProperty(property) && metaPropsToRemove.contains(property)) {
                if ((<any>metaDto)[property] && removedProps) {
                    removedProps.push({ name: property, value: (<any>metaDto)[property].toString() });
                }
                delete (<any>metaDto)[property];
            }
        }
        return metaDto;

    }
}

export = documentMetadata;
