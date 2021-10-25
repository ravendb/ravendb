const $ = require('jquery');
const system = require('durandal/system');
const viewLocator = require('durandal/viewLocator');
const viewEngine = require("durandal/viewEngine");

export function overrideViews() {
// Allow using `function` or bare HTML string as a view
    const locateView = viewLocator.locateView;
    viewLocator.locateView = function(viewOrUrlOrId: any, area: string) {
        // HTML here will be passed into `processMarkup`
        const possibleViewToUse = viewOrUrlOrId && viewOrUrlOrId.default ? viewOrUrlOrId.default : viewOrUrlOrId;
        
        if ("string" === typeof possibleViewToUse && $.trim(possibleViewToUse).charAt(0) === '<') {
            return system.defer(function(dfd: DurandalDeferred<any>) {
                const element = viewEngine.processMarkup(possibleViewToUse);
                dfd.resolve(element.cloneNode(true));
            });
        }

        // super()
        return locateView.apply(this, arguments);
    };


    const isViewUrl = viewEngine.isViewUrl;

    viewEngine.isViewUrl = function(url: string) {
        if (!url || url === "views/") {
            return false;
        }
        if (url && url.startsWith("<")) {
            return true;
        }
        
        return isViewUrl(url);
    };
}

