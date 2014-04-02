class querySort {
    fieldName = ko.observable<string>();
    fieldNameOrDefault: KnockoutComputed<string>;
    ascending: KnockoutComputed<boolean>;
    descending: KnockoutComputed<boolean>;
    rangeAscending: KnockoutComputed<boolean>;
    rangeDescending: KnockoutComputed<boolean>;
    sortDirection = ko.observable(0); // 0 = ascending, 1 = descending, 2 = range ascending, 3 = range descending
    isRange = ko.observable<boolean>(false);
    isAscending= ko.observable<boolean>(true);

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

        if (this.isRange() === true) {

            if (this.isAscending() === true) {
                return this.fieldName() + querySort.rangeIndicator; //ascending range
            }
            else {
                return "-" + this.fieldName() + querySort.rangeIndicator; //descending range
            }
        } else {

            if (this.isAscending() === true) {
                return this.fieldName(); // ascending
            }
            else {
                return "-" + this.fieldName(); // descending
            }
        }
    }

    toHumanizedString(): string {

        if (this.isRange() === true) {

            if (this.isAscending() === true) {
                return this.fieldName() + " range"; //ascending range
            }
            else {
                return this.fieldName() + " range descending"; //descending range
            }
        } else {

            if (this.isAscending() === true) {
                return this.fieldName(); // ascending
            }
            else {
                return this.fieldName() + " descending"; // descending
            }
        }
      
    }

    static fromQuerySortString(querySortText: string) {
        var sortDirection = 0;
        var sortField = "";

        var isDescending = querySortText.slice(0, 1) === "-";
        var isRange = querySortText.length > querySort.rangeIndicator.length && querySortText.substr(querySortText.length - querySort.rangeIndicator.length) === querySort.rangeIndicator;
        if (isRange && isDescending) {
            sortField = querySortText.substr(1, querySortText.length - 1 - querySort.rangeIndicator.length);
        } else if (isRange && !isDescending) {
            sortField = querySortText.substr(0, querySortText.length - 1 - querySort.rangeIndicator.length);
        } else if (!isRange && isDescending) {
            sortField = querySortText.substr(1, querySortText.length - 1);
        } else {
            sortField = querySortText;
        }

        var q = new querySort();
        q.isAscending(!isDescending);
        q.isRange(isRange);
        q.fieldName(sortField);
        return q;
    }
}

export = querySort; 