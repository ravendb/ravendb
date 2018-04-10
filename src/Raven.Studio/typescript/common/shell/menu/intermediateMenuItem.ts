/// <reference path="../../../../typings/tsd.d.ts" />

class intermediateMenuItem implements menuItem {
    title: string;
    children: menuItem[];
    css: string;
    readonly type: menuItemType = "intermediate";
    isOpen: KnockoutObservable<boolean> = ko.observable(false);
    parent: KnockoutObservable<menuItem> = ko.observable(null);
    hash: string;
    dynamicHash: dynamicHashType;
    path: KnockoutObservable<string>;
    nav: KnockoutObservable<boolean> = ko.observable(true);

    depth: KnockoutComputed<number> = ko.pureComputed(() => {
        let next = this.parent(),
            result = 0;
        while (next) {
            result += 1;
            next = next.parent();
        }

        return result;
    });

    constructor(title: string, children: menuItem[], css?: string, hashes?: { dynamicHash: () => string, hash?: string }, nav?: boolean | KnockoutObservable<boolean>) {
        this.title = title;
        this.children = children;
        this.css = css;
        
        if (nav !== undefined) {
            if (ko.isObservable(nav)) {
                this.nav = nav;
            } else {
                this.nav(nav);
            }    
        }
       
        if (hashes && hashes.dynamicHash) {
            this.dynamicHash = hashes.dynamicHash;
        }

        if (hashes && hashes.hash) {
            this.hash = hashes.hash;
        }

        if (this.children && this.children.length) {
            this.children.forEach(x => x.parent(this));
        }

        this.path = ko.pureComputed(() => {
            if (this.hash) {
                return this.hash;
            } else if (this.dynamicHash) {
                return this.dynamicHash();
            }

            return null;
        });
    }

    open() {
        this.isOpen(true);
    }

    close() {
        this.isOpen(false);
    }
}

export = intermediateMenuItem;
