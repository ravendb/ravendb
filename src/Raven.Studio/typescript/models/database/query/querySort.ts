/// <reference path="../../../../typings/tsd.d.ts"/>

class querySort {

    fieldName = ko.observable<string>();
    sortType = ko.observable<querySortType>("Ascending");

    static readonly RangeIndicator = "_Range";

    static empty() {
        return new querySort();
    }

    bindOnUpdateAction(func: () => void) {
        this.fieldName.subscribe(() => func());
        this.sortType.subscribe(() => func());
    }

    toQuerySortString(): string {
        switch (this.sortType()) {
            case "Ascending":
                return this.fieldName();
            case "Descending":
                return "-" + this.fieldName();
            case "Range Ascending":
                return this.fieldName() + querySort.RangeIndicator;
            case "Range Descending":
                return "-" + this.fieldName() + querySort.RangeIndicator;
        }
    }

    toHumanizedString(): string {
        switch (this.sortType()) {
            case "Ascending":
                return this.fieldName() + " ascending"
            case "Descending":
                return this.fieldName() + " descending";
            case "Range Ascending":
                return this.fieldName() + " range";
            case "Range Descending":
                return this.fieldName() + " range descending";
        }
    }

    static fromQuerySortString(querySortText: string): querySort {
        const isDescending = querySortText.startsWith("-");
        const isRange = querySortText.endsWith(querySort.RangeIndicator);

        let rawSortText = querySortText;
        if (isDescending) {
            rawSortText = rawSortText.substr(1);
        }
        if (isRange) {
            rawSortText = rawSortText.substr(0, rawSortText.length - querySort.RangeIndicator.length);
        }

        const newSort = querySort.empty();
        newSort.fieldName(rawSortText);
        newSort.sortType(querySort.getSortType(isDescending, isRange));

        return newSort;
    }

    private static getSortType(isDescending: boolean, isRange: boolean) {
        if (isDescending) {
            return isRange ? "Range Descending" : "Descending";
        } else { //asc
            return isRange ? "Range Ascending" : "Ascending";
        }
    }
}

export = querySort; 
