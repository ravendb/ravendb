import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");

class leafMenuItem implements menuItem {
    title: string;
    tooltip: string;
    nav: boolean;
    route: string | Array<string>;
    moduleId: string;
    hash: string;
    dynamicHash: dynamicHashType;
    css: string;
    openAsDialog: boolean;
    path: KnockoutComputed<string>;
    parent: KnockoutObservable<intermediateMenuItem> = ko.observable(null);
    enabled: KnockoutObservable<boolean>;
    type: menuItemType = "leaf";

    constructor({ title, tooltip, route, moduleId, nav, hash, css, dynamicHash, enabled, openAsDialog }: {
        title: string,
        route: string | Array<string>,
        moduleId: string,
        nav: boolean,
        tooltip?: string,
        hash?: string,
        dynamicHash?: dynamicHashType,
        css?: string,
        openAsDialog?: boolean,
        enabled?: KnockoutObservable<boolean>;
    }) {
        if (nav && !hash && !dynamicHash && !openAsDialog) {
            console.error("Invalid route configuration:" + title);
        }

        this.title = title;
        this.route = route;
        this.moduleId = moduleId;
        this.nav = nav;
        this.hash = hash;
        this.dynamicHash = dynamicHash;
        this.css = css;
        this.enabled = enabled;
        this.openAsDialog = openAsDialog;
        this.path = ko.computed(() => {
            if (this.hash) {
                return this.hash;
            } else if (this.dynamicHash) {
                return this.dynamicHash();
            }

            return null;
        });
    }
}

export = leafMenuItem;