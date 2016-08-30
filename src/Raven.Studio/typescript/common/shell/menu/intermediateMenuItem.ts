class intermediateMenuItem implements menuItem {
    title: string;
    children: menuItem[];
    css: string;
    type: menuItemType = "intermediate";

    constructor(title: string, children: menuItem[], css?: string) {
        this.title = title;
        this.children = children;
        this.css = css;
    }
}

export = intermediateMenuItem;