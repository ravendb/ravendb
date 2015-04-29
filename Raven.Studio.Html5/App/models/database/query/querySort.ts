class querySort {
    fieldName = ko.observable<string>();
    fieldNameOrDefault: KnockoutComputed<string>;
    ascending: KnockoutComputed<boolean>;
    descending: KnockoutComputed<boolean>;
    rangeAscending: KnockoutComputed<boolean>;
    rangeDescending: KnockoutComputed<boolean>;
    sortDirection = ko.observable(0); // 0 = ascending, 1 = descending, 2 = range ascending, 3 = range descending
    isRange = ko.observable<boolean>(false);
    isAscending = ko.observable<boolean>(true);
    isAlphanumeric = ko.observable<boolean>(false);

    static rangeIndicator = "_Range";
    static alphaNumericIndicator = "__alphaNumeric";

    constructor() {
        this.fieldNameOrDefault = ko.computed(() => this.fieldName() ? this.fieldName() : "Select a field");
        this.isAlphanumeric.subscribe((value) => {
            if (value && this.isRange()) {
                this.isRange(false);
            }
        });
    }

    toggleAscending() {
        this.isAscending.toggle();
    }

    toggleRange() {
        this.isRange.toggle();
    }
  
    setAlphaNumeric(value: boolean) {
        this.isAlphanumeric(value);
    }

    toQuerySortString(): string {
        var querySortString: string;

        if (this.isAlphanumeric()) {
            if (this.isAscending()) {
                querySortString = querySort.alphaNumericIndicator + ";" + this.fieldName(); //ascending alphanumeric
            } else {
                querySortString = "-" + querySort.alphaNumericIndicator + ";" + this.fieldName(); //descending alphanumeric
            }
        }
        else if (this.isRange()) {
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

        if (this.isAlphanumeric()) {
            if (this.isAscending()) {
                str = this.fieldName() + " alphanumeric"; //ascending alphanumeric
            } else {
                str = this.fieldName() + " alphanumeric descending"; //descending alphanumeric
            }
        }
        else if (this.isRange()) {
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
        var isAlphanumeric = querySortText.length > querySort.alphaNumericIndicator.length
            && (querySortText.substr(0, querySort.alphaNumericIndicator.length) === querySort.alphaNumericIndicator || 
            querySortText.substr(1, querySort.alphaNumericIndicator.length) === querySort.alphaNumericIndicator);
        var isRange = querySortText.length > querySort.rangeIndicator.length 
            && querySortText.substr(querySortText.length - querySort.rangeIndicator.length) === querySort.rangeIndicator;

        var sortField = querySort.getSortField(querySortText, isDescending, isAlphanumeric, isRange);

        var q = new querySort();
        q.isAscending(!isDescending);
        q.isAlphanumeric(isAlphanumeric);
        q.isRange(isRange);
        q.fieldName(sortField);
        return q;
    }

    private static getSortField(querySortText: string, isDescending: boolean, isAlphanumeric: boolean, isRange: boolean) {
        var sortField: string;

        if (isAlphanumeric && isDescending) {
            sortField = querySortText.substr(querySort.alphaNumericIndicator.length + 2);
        }
        else if (isAlphanumeric && !isDescending) {
            sortField = querySortText.substr(querySort.alphaNumericIndicator.length + 1);
        }
        else if (isRange && isDescending) {
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