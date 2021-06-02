class perNodeStatItems<TItem> {
    readonly tag: string;
    disconnected = ko.observable<boolean>(true);

    items: TItem[] = [];

    constructor(tag: string) {
        this.tag = tag;
    }
}

export = perNodeStatItems;
