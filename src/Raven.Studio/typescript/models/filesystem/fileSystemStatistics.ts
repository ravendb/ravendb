/// <reference path="../../../typings/tsd.d.ts"/>

class fileSystemStatistics {
    fileCountText = ko.observable<string>();
    requestsCountText = ko.observable<string>();
    meanDurationText = ko.observable<string>();
    
    fromDto(dto: filesystemStatisticsDto) {
        this.fileCountText(this.getItemCountText(dto.FileCount, "file", "s"));
        this.requestsCountText(dto.Metrics.RequestsPerSecond + " requests per second");
        this.meanDurationText(dto.Metrics.RequestsDuration.Mean.toFixed(1) + "ms mean duration");
    }

    private getItemCountText(itemCount: number, singularText: string, suffix: string): string {
        var itemCountText = itemCount.toLocaleString() + " " + singularText;
        if (itemCount !== 1) {
            itemCountText += suffix;
        }
        return itemCountText;
    }
}

export = fileSystemStatistics;
