/// <reference path="../../../../typings/tsd.d.ts" />

class pluralizeHelpers {
    static pluralize(count: number, singular: string, plural: string, onlyPostfix: boolean = false) {
        if (typeof(count) === "string") {
            count = parseInt(count);
        }
        
        if (onlyPostfix) {
            return count === 1 ? singular : plural;
        } else {
            return count === 1 ? count.toLocaleString() + " " + singular : count.toLocaleString() + " " + plural;
        }
    }
}

export = pluralizeHelpers;
