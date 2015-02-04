class fileSystemStatistics {
    fileCount = ko.observable<number>();

    fileCountText = ko.observable<string>();
    requestsCountText = ko.observable<string>();
    meanDurationText = ko.observable<string>();

    countOfIndexesText = ko.observable<string>();
    staleIndexesCountText = ko.observable<string>();
    errorsCountText = ko.observable<string>();
    
    constructor(dto: filesystemStatisticsDto) {
        this.fileCount(dto.FileCount);

        this.fileCountText(this.getItemCountText(dto.FileCount, "file", "s"));
        this.requestsCountText(dto.Metrics.RequestsPerSecond + " requests per second");
        this.meanDurationText(dto.Metrics.RequestsDuration.Mean.toFixed(1) + 'ms mean duration');
    }

    private getItemCountText(itemCount: number, singularText: string, suffix: string): string {
        var itemCountText = itemCount.toLocaleString() + ' ' + singularText;
        if (itemCount != 1) {
            itemCountText += suffix;
        }
        return itemCountText;
    }
}

export = fileSystemStatistics;