class fileMetadata {
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
            this.ravenFSSize = dto['ravenFS-Size'];
            this.ravenSynchronizationHistory = dto['Raven-Synchronization-History'];
            this.ravenSynchronizationVersion = dto['Raven-Synchronization-Version'];
            this.ravenSynchronizationSource = dto['Raven-Synchronization-Source'];
            this.lastModified = dto['Last-Modified'];
            this.etag = dto['Etag'];
        }
    }
}

export = fileMetadata; 