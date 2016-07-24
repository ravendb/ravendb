/// <reference path="../../../../typings/tsd.d.ts" />

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


    static filterMetadata(metaDto: documentMetadataDto, removedProps: any[] = null) {
        // We don't want to show certain reserved properties in the metadata text area.
        // Remove them from the DTO, restore them on save.
        //TODO: that's the original list, since 4.0 we start from scratch as now we don't store metadata as headers, so some of header won't have appliance
        /*
        var metaPropsToRemove = ["@id", "@etag", "Origin", "Raven-Server-Build", "Raven-Client-Version", "Non-Authoritative-Information", "Raven-Timer-Request",
            "Raven-Authenticated-User", "Raven-Last-Modified", "Has-Api-Key", "Access-Control-Allow-Origin", "Access-Control-Max-Age", "Access-Control-Allow-Methods",
            "Access-Control-Request-Headers", "Access-Control-Allow-Headers", "Reverse-Via", "Persistent-Auth", "Allow", "Content-Disposition", "Content-Encoding",
            "Content-Language", "Content-Location", "Content-MD5", "Content-Range", "Content-Type", "Expires", "Last-Modified", "Content-Length", "Keep-Alive", "X-Powered-By",
            "X-AspNet-Version", "X-Requested-With", "X-SourceFiles", "Accept-Charset", "Accept-Encoding", "Accept", "Accept-Language", "Authorization", "Cookie", "Expect",
            "From", "Host", "If-MatTemp-Index-Scorech", "If-Modified-Since", "If-None-Match", "If-Range", "If-Unmodified-Since", "Max-Forwards", "Referer", "TE", "User-Agent", "Accept-Ranges",
            "Age", "Allow", "ETag", "Location", "Retry-After", "Server", "Set-Cookie2", "Set-Cookie", "Vary", "Www-Authenticate", "Cache-Control", "Connection", "Date", "Pragma",
            "Trailer", "Transfer-Encoding", "Upgrade", "Via", "Warning", "X-ARR-LOG-ID", "X-ARR-SSL", "X-Forwarded-For", "X-Original-URL", "Size-In-Kb"];*/
        var metaPropsToRemove = ["@id", "@etag", "Raven-Last-Modified"];

        for (var property in metaDto) {
            if (metaDto.hasOwnProperty(property) && metaPropsToRemove.contains(property)) {
                if (metaDto[property] && removedProps) {
                    removedProps.push({ name: property, value: metaDto[property].toString() });
                }
                delete metaDto[property];
            }
        }
        return metaDto;

    }
}

export = documentMetadata;
