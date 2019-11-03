interface compressionLevelOption {
    name: string;
    fullName: System.IO.Compression.CompressionLevel;
}

class snapshot {
    static readonly compressionLevelDictionary: compressionLevelOption[] = [
        { name: "Optimal", fullName: "Optimal" },
        { name: "Fastest", fullName: "Fastest" },
        { name: "No Compression", fullName: "NoCompression" }
    ];

    compressionLevelOptions = snapshot.compressionLevelDictionary.map(x => x.name);

    compressionLevel = ko.observable<string>();

    constructor(dto: Raven.Client.Documents.Operations.Backups.SnapshotSettings) {
        const compressionLevel = snapshot.compressionLevelDictionary.find(x => x.fullName === dto.CompressionLevel);
        this.compressionLevel(compressionLevel.name);
    }

    useCompression(option: string) {
        this.compressionLevel(option);
    }

    toDto(): Raven.Client.Documents.Operations.Backups.SnapshotSettings {
        const compressionLevel = snapshot.compressionLevelDictionary.find(x => x.name === this.compressionLevel());

        return {
            CompressionLevel: compressionLevel.fullName
        }
    }

    static empty(): snapshot {
        return new snapshot({
            CompressionLevel: "Optimal"
        });
    }
}

export = snapshot;
