class fileMetadata {

    standardProps = ["RavenFs-Size","Raven-Synchronization-History", "Raven-Synchronization-Version", "Raven-Synchronization-Source", "Last-Modified", "ETag"];

    ravenFSSize: string;
    ravenSynchronizationHistory: string;
    ravenSynchronizationVersion: string;
    ravenSynchronizationSource: string;
    lastModified: string;
    etag: string;
    nonStandardProps: Array<string>;

    constructor(dto?: any) {
        if (dto) {
            this.ravenFSSize = dto['RavenFS-Size'];
            this.ravenSynchronizationHistory = dto['Raven-Synchronization-History'];
            this.ravenSynchronizationVersion = dto['Raven-Synchronization-Version'];
            this.ravenSynchronizationSource = dto['Raven-Synchronization-Source'];
            this.lastModified = dto['Last-Modified'];
            this.etag = dto['ETag'];

            for (var property in dto) {
                if (!this.standardProps.contains(property)) {
                    this.nonStandardProps = this.nonStandardProps || [];
                    this[property] = dto[property];
                    this.nonStandardProps.push(property);
                }
            }
        }
    }

    toDto(): fileMetadataDto {
        var dto: any = {
            'Last-Modified': this.lastModified,
            'ETag': this.etag,
            'Raven-Synchronization-History': this.ravenSynchronizationHistory,
            'Raven-Synchronization-Version': this.ravenSynchronizationVersion,
            'Raven-Synchronization-Source': this.ravenSynchronizationSource,
            'RavenFS-Size': this.ravenFSSize,
        };

        if (this.nonStandardProps) {
            this.nonStandardProps.forEach(p => dto[p] = this[p]);
        }

        return dto;
    }
}

export = fileMetadata; 