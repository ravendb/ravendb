class querySort {
    fieldName = ko.observable<string>();
    fieldNameOrDefault: KnockoutComputed<string>;
    ascending: KnockoutComputed<boolean>;
    descending: KnockoutComputed<boolean>;
    rangeAscending: KnockoutComputed<boolean>;
    rangeDescending: KnockoutComputed<boolean>;
    sortDirection = ko.observable(0); // 0 = ascending, 1 = descending, 2 = range ascending, 3 = range descending

    static rangeIndicator = "_Range";

    constructor() {
        this.fieldNameOrDefault = ko.computed(() => this.fieldName() ? this.fieldName() : "Select a field");
        this.ascending = this.makeStatusComputed(0);
        this.descending = this.makeStatusComputed(1);
        this.rangeAscending = this.makeStatusComputed(2);
        this.rangeDescending = this.makeStatusComputed(3);
    }

    toQuerySortString(): string {
        if (this.descending()) {
            return "-" + this.fieldName();
        }
        if (this.rangeAscending()) {
            return this.fieldName() + querySort.rangeIndicator;
        }
        if (this.rangeDescending()) {
            return "-" + this.fieldName() + querySort.rangeIndicator;
        }

        return this.fieldName();
    }

    static fromQuerySortString(querySortText: string) {
        var sortDirection = 0;
        var sortField = "";

        var isDescending = querySortText.slice(0, 1) === "-";
        var isRange = querySortText.length > querySort.rangeIndicator.length && querySortText.substr(querySortText.length - querySort.rangeIndicator.length) === querySort.rangeIndicator;
        if (isRange && isDescending) {
            sortDirection = 3;
            sortField = querySortText.substr(1, querySortText.length - 1 - querySort.rangeIndicator.length);
        } else if (isRange && !isDescending) {
            sortDirection = 2;
            sortField = querySortText.substr(0, querySortText.length - 1 - querySort.rangeIndicator.length);
        } else if (!isRange && isDescending) {
            sortDirection = 1;
            sortField = querySortText.substr(1, querySortText.length - 1);
        } else {
            sortDirection = 0;
            sortField = querySortText;
        }

        var q = new querySort();
        q.sortDirection(sortDirection);
        q.fieldName(sortField);
        return q;
    }

    private makeStatusComputed(status: number): KnockoutComputed<boolean> {
        return ko.computed({
            read: () => this.sortDirection() === status,
            write: (val: boolean) => {
                if (val) {
                    this.sortDirection(status);
                }
            }
        });
    }
}

export = querySort; 