class timeSeriesStatistics {
    timeSeriesCount = ko.observable<number>();
    timeSeriesCountText = ko.observable<string>("");
    requestsPerSecondText = ko.observable<string>("");

    private getItemCountText(itemCount: number, singularText: string, suffix: string): string {
        var itemCountText = itemCount.toLocaleString() + " " + singularText;
        if (itemCount !== 1) {
            itemCountText += suffix;
        }
        return itemCountText;
    }

    public fromDto(dto: timeSeriesStatisticsDto) {
        this.timeSeriesCount(dto.TimeSeriesCount);
        this.timeSeriesCountText(this.getItemCountText(dto.TimeSeriesCount, "time series", "es"));
        this.requestsPerSecondText(dto.RequestsPerSecond + " requests per second");
    }
}

export = timeSeriesStatistics;