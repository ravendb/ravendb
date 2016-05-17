/// <reference path="../../../typings/tsd.d.ts"/>

class databaseStatistics {
    countOfDocuments = ko.observable<number>();
    databaseId = ko.observable<string>();

    countOfDocumentsText = ko.observable<string>();
    countOfIndexesText = ko.observable<string>();
    staleFilteredIndexesCountText = ko.observable<string>();
    countOfFilteredIndexesText = ko.observable<string>();
    staleIndexesCountText = ko.observable<string>();
    errorsCountText = ko.observable<string>();
    
    fromDto(dto: databaseStatisticsDto) {
        this.countOfDocuments(dto.CountOfDocuments);
        this.databaseId(dto.DatabaseId);


        this.countOfFilteredIndexesText("0 indexes"); //TODO: this.getItemCountText(dto.CountOfIndexesExcludingDisabledAndAbandoned, "index", "es")
        this.staleFilteredIndexesCountText("0 stale"); //TODO: dto.CountOfStaleIndexesExcludingDisabledAndAbandoned.toLocaleString() + " stale"
        this.errorsCountText("0 errors"); //TODO: this.getItemCountText(dto.Errors.length, "error", "s")
        this.countOfDocumentsText("0 documents"); //TODO: this.getItemCountText(dto.CountOfDocuments, "document", "s")
        this.countOfIndexesText("0 indexes"); //TODO: this.getItemCountText(dto.CountOfIndexes, "index", "es")
        this.staleIndexesCountText("0 stale"); //TODO: dto.StaleIndexes.length.toLocaleString() + " stale"
        this.errorsCountText("0 errors"); //TODO: this.getItemCountText(dto.Errors.length, "error", "s")
    }

    private getItemCountText(itemCount: number, singularText: string, suffix: string): string {
        var itemCountText = itemCount.toLocaleString() + " " + singularText;
        if (itemCount !== 1) {
            itemCountText += suffix;
        }
        return itemCountText;
    }
}

export = databaseStatistics;
