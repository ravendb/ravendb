class querySort {
    fieldName = ko.observable<string>();
    fieldNameOrDefault: KnockoutComputed<string>;
    ascending: KnockoutComputed<boolean>;
    descending: KnockoutComputed<boolean>;
    rangeAscending: KnockoutComputed<boolean>;
    rangeDescending: KnockoutComputed<boolean>;
    sortDirection = ko.observable(0); // 0 = ascending, 1 = descending, 2 = range ascending, 3 = range descending

    constructor() {
        this.fieldNameOrDefault = ko.computed(() => this.fieldName() ? this.fieldName() : "Select a field");
        this.ascending = this.makeStatusComputed(0);
        this.descending = this.makeStatusComputed(1);
        this.rangeAscending = this.makeStatusComputed(2);
        this.rangeDescending = this.makeStatusComputed(3);
    }

    public toQuerySortString(): string {
        if (this.descending()) {
            return "-" + this.fieldName();
        }
        if (this.rangeAscending()) {
            return this.fieldName() + "_Range";
        }
        if (this.rangeDescending()) {
            return "-" + this.fieldName() + "_Range";
        }

        return this.fieldName();
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