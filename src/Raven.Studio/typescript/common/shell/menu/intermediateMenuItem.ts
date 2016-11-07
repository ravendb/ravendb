class intermediateMenuItem implements menuItem {
    title: string;
    children: menuItem[];
    css: string;
    type: menuItemType = "intermediate";
    isOpen: KnockoutObservable<boolean> = ko.observable(false);
    parent: KnockoutObservable<menuItem> = ko.observable(null);
    depth: KnockoutComputed<number> = ko.computed(() => {
        let next = this.parent(),
            result = 0;
        while (next) {
            result += 1;
            next = next.parent();
        }

        return result;
    });

    constructor(title: string, children: menuItem[], css?: string) {
        this.title = title;
        this.children = children;
        this.css = css;

        if (this.children && this.children.length) {
            this.children.forEach(x => x.parent(this));
        }
    }

    open() {
        this.isOpen(true);
    }

    close() {
        this.isOpen(false);
    }
}

export = intermediateMenuItem;