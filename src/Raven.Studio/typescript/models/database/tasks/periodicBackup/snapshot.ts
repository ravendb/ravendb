interface compressionLevelOption {
    name: string;
    fullName: System.IO.Compression.CompressionLevel;
}

interface compressionAlgorithmLevelOption {
    name: string;
    fullName: Sparrow.Backups.SnapshotBackupCompressionAlgorithm;
}

class snapshot {
    static readonly compressionLevelDictionary: compressionLevelOption[] = [
        { name: "Optimal", fullName: "Optimal" },
        { name: "Fastest", fullName: "Fastest" },
        { name: "No Compression", fullName: "NoCompression" },
        { name: "Smallest Size", fullName: "SmallestSize" }
    ];

    static readonly compressionAlgorithmDictionary: compressionAlgorithmLevelOption[] = [
        { name: "Zstd", fullName: "Zstd" },
        { name: "Deflate", fullName: "Deflate" }
    ];

    compressionLevelOptions = snapshot.compressionLevelDictionary.map(x => x.name);

    compressionAlgorithmOptions = snapshot.compressionAlgorithmDictionary.map(x => x.name);

    compressionLevel = ko.observable<string>();

    compressionAlgorithm = ko.observable<string>();

    excludeIndexes = ko.observable<boolean>(false);
    
    constructor(dto: Raven.Client.Documents.Operations.Backups.SnapshotSettings) {
        const compressionLevel = snapshot.compressionLevelDictionary.find(x => x.fullName === dto.CompressionLevel);
        const compressionAlgorithm = snapshot.compressionAlgorithmDictionary.find(x => x.fullName === dto.CompressionAlgorithm);

        this.compressionLevel(compressionLevel.name);
        this.compressionAlgorithm(compressionAlgorithm.name);
        this.excludeIndexes(dto.ExcludeIndexes);
    }

    useCompression(option: string) {
        this.compressionLevel(option);
    }

    useAlgorightm(option: string) {
        this.compressionAlgorithm(option);
    }

    toDto(): Raven.Client.Documents.Operations.Backups.SnapshotSettings {
        const compressionLevel = snapshot.compressionLevelDictionary.find(x => x.name === this.compressionLevel());
        const compressionAlgorighm = snapshot.compressionAlgorithmDictionary.find(x => x.name === this.compressionAlgorithm());

        return {
            CompressionLevel: compressionLevel.fullName,
            CompressionAlgorithm: compressionAlgorighm.fullName,
            ExcludeIndexes: this.excludeIndexes()
        }
    }

    static empty(): snapshot {
        return new snapshot({
            CompressionLevel: "Fastest",
            CompressionAlgorithm: "Zstd",
            ExcludeIndexes: false
        });
    }
}

export = snapshot;
