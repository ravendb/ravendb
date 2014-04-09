class fileMetadata {

    standardProps = ["Origin", "RavenFs-Size",
        "Raven-Synchronization-History", "Raven-Synchronization-Version", "Raven-Synchronization-Source", "Last-Modified", "ETag"];

    origin: string;
    ravenFSSize: string;
    ravenSynchronizationHistory: string;
    ravenSynchronizationVersion: string;
    ravenSynchronizationSource: string;
    lastModified: string;
    etag: string;
    nonStandardProps: Array<string>;

    constructor(dto?: any) {
        if (dto) {
            this.origin = dto['Origin'];
            this.ravenFSSize = dto['RavenFS-Size'];
            this.ravenSynchronizationHistory = dto['Raven-Synchronization-History'];
            this.ravenSynchronizationVersion = dto['Raven-Synchronization-Version'];
            this.ravenSynchronizationSource = dto['Raven-Synchronization-Source'];
            this.lastModified = dto['Last-Modified'];
            this.etag = dto['ETag'];

            for (var property in dto) {
                if (this.standardProps.contains(property)) {
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
            '@etag': this.etag,
            'Raven-Synchronization-History': this.ravenSynchronizationHistory,
            'Raven-Synchronization-Version': this.ravenSynchronizationVersion,
            'Raven-Synchronization-Source': this.ravenSynchronizationSource,
            'RavenFS-Size': this.ravenFSSize,
            'Origin': this.origin
        };

        if (this.nonStandardProps) {
            this.nonStandardProps.forEach(p => dto[p] = this[p]);
        }

        return dto;
    }
}

export = fileMetadata; 