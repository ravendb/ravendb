/// <reference path="../../../../typings/tsd.d.ts" />

class pluralizeHelpers {
    static pluralize(count: number, singular: string, plural: string) {
        return count === 1 ? count.toLocaleString() + " " + singular : count.toLocaleString() + " " + plural;
    }
}

export = pluralizeHelpers;
