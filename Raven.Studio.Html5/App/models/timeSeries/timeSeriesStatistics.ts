class timeSeriesStatistics {
    prefixesCount = ko.observable<number>();
    keysCount = ko.observable<number>();
    valuesCount = ko.observable<number>();
    prefixesCountText = ko.observable<string>("");
    keysCountText = ko.observable<string>("");
    valuesCountText = ko.observable<string>("");
    requestsPerSecondText = ko.observable<string>("");

    private getItemCountText(itemCount: number, singularText: string, suffix: string): string {
        var itemCountText = itemCount.toLocaleString() + " " + singularText;
        if (itemCount !== 1) {
            itemCountText += suffix;
        }
        return itemCountText;
    }

    public fromDto(dto: timeSeriesStatisticsDto) {
        this.prefixesCount(dto.PrefixesCount);
        this.keysCount(dto.KeysCount);
        this.valuesCount(dto.ValuesCount);
        this.prefixesCountText(this.getItemCountText(dto.PrefixesCount, "prefix", "es"));
        this.keysCountText(this.getItemCountText(dto.KeysCount, "key", "s"));
        this.valuesCountText(this.getItemCountText(dto.ValuesCount, "value", "s"));
        this.requestsPerSecondText(dto.RequestsPerSecond + " requests per second");
    }
}

export = timeSeriesStatistics;