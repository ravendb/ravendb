/// <reference path="../models/dto.ts" />

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

    constructor(dto?: documentMetadataDto) {
        if (dto) {
            this.ravenEntityName = dto['Raven-Entity-Name'];
            this.ravenClrType = dto['Raven-Clr-Type'];
            this.nonAuthoritativeInfo = dto['Non-Authoritative-Information'];
            this.id = dto['@id'];
            this.tempIndexScore = dto['Temp-Index-Score'];
            this.lastModified = dto['Last-Modified'];
            this.ravenLastModified = dto['Raven-Last-Modified'];
            this.etag = dto['@etag'];

            for (var property in dto) {
                if (this.shouldBeIncluded(property)) {
                    this.nonStandardProps = this.nonStandardProps || [];
                    this[property] = dto[property];
                    this.nonStandardProps.push(property);
                }
            }
        }
    }

    private shouldBeIncluded(property): boolean {
        // skip all properties which are included directly
        var directlyIncluded = [
            'Raven-Entity-Name',
            'Raven-Clr-Type',
            'Non-Authoritative-Information',
            '@id',
            'Temp-Index-Score',
            'Last-Modified',
            'Raven-Last-Modified',
            '@etag'
        ];
        var extraSkipped = [
            'Raven-Replication-History'
        ];
        var skipped = jQuery.merge(directlyIncluded, extraSkipped);
        return (jQuery.inArray(property, skipped) === -1);
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
            this.nonStandardProps.forEach(p => {
                dto[p] = this[p];
            });
        }

        return dto;
    }
}

export = documentMetadata;