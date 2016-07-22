/// <reference path="../../../../typings/tsd.d.ts"/>

class querySort {
    fieldName = ko.observable<string>();
    fieldNameOrDefault: KnockoutComputed<string>;
    ascending: KnockoutComputed<boolean>;
    descending: KnockoutComputed<boolean>;
    rangeAscending: KnockoutComputed<boolean>;
    rangeDescending: KnockoutComputed<boolean>;
    isRange = ko.observable<boolean>(false);
    isAscending = ko.observable<boolean>(true);

    static rangeIndicator = "_Range";

    constructor() {
        this.fieldNameOrDefault = ko.computed(() => this.fieldName() ? this.fieldName() : "Select a field");
    }

    toggleAscending() {
        this.isAscending.toggle();
    }

    toggleRange() {
        this.isRange.toggle();
    }

    toQuerySortString(): string {
        var querySortString: string;

        if (this.isRange()) {
            if (this.isAscending()) {
                querySortString = this.fieldName() + querySort.rangeIndicator; //ascending range
            }
            else {
                querySortString = "-" + this.fieldName() + querySort.rangeIndicator; //descending range
            }
        } else {
            if (this.isAscending()) {
                querySortString = this.fieldName(); // ascending
            }
            else {
                querySortString = "-" + this.fieldName(); // descending
            }
        }

        return querySortString;
    }

    toHumanizedString(): string {
        var str;

        if (this.isRange()) {
            if (this.isAscending()) {
                str = this.fieldName() + " range"; //ascending range
            }
            else {
                str = this.fieldName() + " range descending"; //descending range
            }
        } else {

            if (this.isAscending()) {
                str = this.fieldName(); // ascending
            }
            else {
                str = this.fieldName() + " descending"; // descending
            }
        }

        return str;
    }

    static fromQuerySortString(querySortText: string) {
        var isDescending = querySortText.slice(0, 1) === "-";
        var isRange = querySortText.length > querySort.rangeIndicator.length 
            && querySortText.substr(querySortText.length - querySort.rangeIndicator.length) === querySort.rangeIndicator;

        var sortField = querySort.getSortField(querySortText, isDescending, isRange);

        var q = new querySort();
        q.isAscending(!isDescending);
        q.isRange(isRange);
        q.fieldName(sortField);
        return q;
    }

    private static getSortField(querySortText: string, isDescending: boolean, isRange: boolean) {
        var sortField: string;

        if (isRange && isDescending) {
            sortField = querySortText.substr(1, querySortText.length - 1 - querySort.rangeIndicator.length);
        }
        else if (isRange && !isDescending) {
            sortField = querySortText.substr(0, querySortText.length - 1 - querySort.rangeIndicator.length);
        }
        else if (!isRange && isDescending) {
            sortField = querySortText.substr(1, querySortText.length - 1);
        }
        else {
            sortField = querySortText;
        }

        return sortField;
    }
}

export = querySort; 
