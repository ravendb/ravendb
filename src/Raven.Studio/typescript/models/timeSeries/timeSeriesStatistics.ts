/// <reference path="../../../typings/tsd.d.ts"/>

class timeSeriesStatistics {
    typesCount = ko.observable<number>();
    keysCount = ko.observable<number>();
    pointsCount = ko.observable<number>();
    typesCountText = ko.observable<string>("");
    keysCountText = ko.observable<string>("");
    pointsCountText = ko.observable<string>("");
    requestsPerSecondText = ko.observable<string>("");
    timeSeriesSize = ko.observable<string>("");

    private getItemCountText(itemCount: number, singularText: string, suffix: string): string {
        var itemCountText = itemCount.toLocaleString() + " " + singularText;
        if (itemCount !== 1) {
            itemCountText += suffix;
        }
        return itemCountText;
    }

    public fromDto(dto: timeSeriesStatisticsDto) {
        this.typesCount(dto.TypesCount);
        this.keysCount(dto.KeysCount);
        this.pointsCount(dto.PointsCount);
        this.typesCountText(this.getItemCountText(dto.TypesCount, "type", "s"));
        this.keysCountText(this.getItemCountText(dto.KeysCount, "key", "s"));
        this.pointsCountText(this.getItemCountText(dto.PointsCount, "point", "s"));
        this.requestsPerSecondText(dto.RequestsPerSecond + " requests per second");
        this.timeSeriesSize(dto.TimeSeriesSize);
    }
}

export = timeSeriesStatistics;
