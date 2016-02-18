/// <reference path="../../../typings/tsd.d.ts"/>

class counterStorageStatistics {
    countersCount = ko.observable<number>();
    counterCountText = ko.observable<string>("");
    groupCountText = ko.observable<string>("");
    requestsPerSecondText = ko.observable<string>("");

    private getItemCountText(itemCount: number, singularText: string, suffix: string): string {
        var itemCountText = itemCount.toLocaleString() + " " + singularText;
        if (itemCount !== 1) {
            itemCountText += suffix;
        }
        return itemCountText;
    }

    public fromDto(dto: counterStorageStatisticsDto) {
        this.countersCount(dto.CountersCount);
        this.counterCountText(this.getItemCountText(dto.CountersCount, "counter", "s"));
        this.groupCountText(this.getItemCountText(dto.GroupsCount, "group", "s"));
        this.requestsPerSecondText(dto.RequestsPerSecond + " requests per second");
    }
}

export = counterStorageStatistics;
