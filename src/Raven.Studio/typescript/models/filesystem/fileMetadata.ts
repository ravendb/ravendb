/// <reference path="../../../typings/tsd.d.ts"/>

class fileMetadata {

    standardProps = ["ravenfs-size", "raven-last-modified", "etag", "raven-creation-date"];

    // We don't want to keep certain reserved properties in the metadata text area.    
    headerPropsToRemove = ["Origin", "Raven-Server-Build", "Raven-Client-Version", "Non-Authoritative-Information", "Raven-Timer-Request", "Content-Length",
        "Raven-Authenticated-User", "Has-Api-Key", "Access-Control-Allow-Origin", "Access-Control-Max-Age", "Access-Control-Allow-Methods",
        "Access-Control-Request-Headers", "Access-Control-Allow-Headers", "Reverse-Via", "Persistent-Auth", "Allow", "Content-Disposition",
        "Content-Encoding", "Content-Language", "Content-Location", "Content-MD5", "Content-Range", "Content-Type", "Expires", "Keep-Alive",
        "X-Powered-By", "X-AspNet-Version", "X-Requested-With", "X-SourceFiles", "Accept-Charset", "Accept-Encoding", "Accept", "Accept-Language",
        "Authorization", "Cookie", "Expect", "From", "Host", "If-MatTemp-Index-Scorech", "If-Modified-Since", "If-None-Match", "If-Range",
        "If-Unmodified-Since", "Max-Forwards", "Referer", "TE", "User-Agent", "Accept-Ranges", "Age", "Allow", "Location", "Retry-After",
        "Server", "Set-Cookie2", "Set-Cookie", "Vary", "Www-Authenticate", "Cache-Control", "Connection", "Date", "Pragma", "Trailer", "Upgrade",
        "Transfer-Encoding", "Via", "Warning", "X-ARR-LOG-ID", "X-ARR-SSL", "X-Forwarded-For", "X-Original-URL", "Temp-Request-Time", "DNT"]; 

    ravenFSSize: string;
    ravenSynchronizationHistory: any;
    ravenSynchronizationVersion: string;
    ravenSynchronizationSource: string;
    lastModified: string;
    etag: string;
    creationDate: string;
    nonStandardProps: Array<any>;

    constructor(dto?: any) {
        if (dto) {
            this.ravenFSSize = dto['RavenFS-Size'];
            this.creationDate = dto['Raven-Creation-Date'];
            this.lastModified = dto['Raven-Last-Modified'];            
            this.etag = dto['ETag'];
            if (this.etag == null) // HACK: Handle different capitalization of Etag by Firefox.
                this.etag = dto['Etag'];

            if (this.etag.startsWith('"'))
                this.etag = this.etag.slice(1, this.etag.length - 1);

            // Effectively remove all the headers that are not useful as metadata.
            for (var property in dto) {
                if (this.headerPropsToRemove.contains(property))
                    delete dto[property];
            }
                       
            for (var property in dto) {                                                
                if (!this.standardProps.contains(property.toLowerCase())) {
                    this.nonStandardProps = this.nonStandardProps || [];
                    var value = dto[property];
                    this[property] = value;
                    this.nonStandardProps.push(property);
                }
            }
        }
    }

    toDto(): fileMetadataDto {
        var dto: any = {
            'Raven-Creation-Date':this.creationDate,
            'Raven-Last-Modified': this.lastModified,
            'ETag': '"' + this.etag + '"',
            'RavenFS-Size': this.ravenFSSize,
        };

        if (this.nonStandardProps) {
            this.nonStandardProps.forEach(p => {
                dto[p] = this[p]
            });
        }

        return dto;
    }
}

export = fileMetadata; 
