interface compressionLevelOption {
    name: string;
    fullName: System.IO.Compression.CompressionLevel;
}

class snapshot {
    static readonly compressionLevelDictionary: compressionLevelOption[] = [
        { name: "Optimal", fullName: "Optimal" },
        { name: "Fastest", fullName: "Fastest" },
        { name: "No Compression", fullName: "NoCompression" },
        { name: "Smallest Size", fullName: "SmallestSize" }
    ];

    compressionLevelOptions = snapshot.compressionLevelDictionary.map(x => x.name);

    compressionLevel = ko.observable<string>();

    excludeIndexes = ko.observable<boolean>(false);
    
    constructor(dto: Raven.Client.Documents.Operations.Backups.SnapshotSettings) {
        const compressionLevel = snapshot.compressionLevelDictionary.find(x => x.fullName === dto.CompressionLevel);
        this.compressionLevel(compressionLevel.name);
        this.excludeIndexes(dto.ExcludeIndexes);
    }

    useCompression(option: string) {
        this.compressionLevel(option);
    }

    toDto(): Raven.Client.Documents.Operations.Backups.SnapshotSettings {
        const compressionLevel = snapshot.compressionLevelDictionary.find(x => x.name === this.compressionLevel());

        return {
            CompressionLevel: compressionLevel.fullName,
            ExcludeIndexes: this.excludeIndexes()
        }
    }

    static empty(): snapshot {
        return new snapshot({
            CompressionLevel: "Optimal",
            ExcludeIndexes: false
        });
    }
}

export = snapshot;
