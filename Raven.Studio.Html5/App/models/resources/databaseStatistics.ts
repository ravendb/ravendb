class databaseStatistics {
    countOfDocuments = ko.observable<number>();
    databaseId = ko.observable<string>();

    countOfDocumentsText = ko.observable<string>();
    countOfIndexesText = ko.observable<string>();
    staleIndexesCountText = ko.observable<string>();
    errorsCountText = ko.observable<string>();
    
    constructor(dto: databaseStatisticsDto) {
        this.countOfDocuments(dto.CountOfDocuments);
        this.databaseId(dto.DatabaseId);

        this.countOfDocumentsText(this.getItemCountText(dto.CountOfDocuments, 'document', 's'));
        this.countOfIndexesText(this.getItemCountText(dto.CountOfIndexes, 'index', 'es'));
        this.staleIndexesCountText(dto.StaleIndexes.length.toLocaleString() + ' stale');
        this.errorsCountText(this.getItemCountText(dto.Errors.length, 'error', 's'));
    }

    private getItemCountText(itemCount: number, singularText: string, suffix: string): string {
        var itemCountText = itemCount.toLocaleString() + ' ' + singularText;
        if (itemCount != 1) {
            itemCountText += suffix;
        }
        return itemCountText;
    }
}

export = databaseStatistics;