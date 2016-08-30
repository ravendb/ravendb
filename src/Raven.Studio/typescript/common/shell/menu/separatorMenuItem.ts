class separatorMenuItem implements menuItem {
    title: string;
    type: menuItemType = "separator";

    constructor(title?: string) {
        this.title = title;
    }
}

export = separatorMenuItem;