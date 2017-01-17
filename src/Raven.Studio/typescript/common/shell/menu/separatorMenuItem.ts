/// <reference path="../../../../typings/tsd.d.ts" />

class separatorMenuItem implements menuItem {
    title: string;
    type: menuItemType = "separator";
    parent: KnockoutObservable<menuItem> = ko.observable(null);

    constructor(title?: string) {
        this.title = title;
    }
}

export = separatorMenuItem;