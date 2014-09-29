class fileMetadata {

    standardProps = ["RavenFs-Size", "Last-Modified", "ETag", "Creation-Date"];

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
            this.creationDate = dto['Creation-Date'];
            this.lastModified = dto['Last-Modified'];            
            this.etag = dto['ETag'];

            for (var property in dto) {
                if (!this.standardProps.contains(property)) {
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
            'Creation-Date':this.creationDate,
            'Last-Modified': this.lastModified,
            'ETag': this.etag,
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