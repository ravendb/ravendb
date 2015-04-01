/// <reference path="../../../models/dto.ts" />

class documentMetadata {
    ravenEntityName: string;
    ravenClrType: string;
    nonAuthoritativeInfo: boolean;
    id: string;
    tempIndexScore: number;
    lastModified: string;
    ravenLastModified: string;
    etag: string;
    nonStandardProps: Array<string>;

    lastModifiedFullDate: KnockoutComputed<string>;
    now = ko.observable(new Date());

    constructor(dto?: documentMetadataDto) {
        if (dto) {
            this.ravenEntityName = dto['Raven-Entity-Name'];
            this.ravenClrType = dto['Raven-Clr-Type'];
            this.nonAuthoritativeInfo = dto['Non-Authoritative-Information'];
            this.id = dto['@id'];
            this.tempIndexScore = dto['Temp-Index-Score'];
            this.lastModified = dto['Last-Modified'];

            this.lastModifiedFullDate = ko.computed(() => {
                if (!!this.lastModified) {
                    var lastModifiedMoment = moment(this.lastModified);
                    var timeSince = lastModifiedMoment.from(this.now());
                    var fullTimeSinceUtc = lastModifiedMoment.utc().format("DD/MM/YYYY HH:mm (UTC)");
                    return timeSince + " (" + fullTimeSinceUtc + ")";
                }
                return "";
            });
            setInterval(() => this.now(new Date()), 60*1000);

            this.ravenLastModified = dto['Raven-Last-Modified'];
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
                    this[property] = dto[property];
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
            this.nonStandardProps.forEach(p => dto[p] = this[p]);
        }

        return dto;
    }
}

export = documentMetadata;