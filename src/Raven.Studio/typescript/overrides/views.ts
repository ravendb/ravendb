const $ = require('jquery');
const system = require('durandal/system');
const viewLocator = require('durandal/viewLocator');
const viewEngine = require("durandal/viewEngine");

export function overrideViews() {
// Allow using `function` or bare HTML string as a view
    const locateView = viewLocator.locateView;
    viewLocator.locateView = function(viewOrUrlOrId: any, area: string) {
        // HTML here will be passed into `processMarkup`
        if ("string" === typeof viewOrUrlOrId && $.trim(viewOrUrlOrId).charAt(0) === '<') {
            return system.defer(function(dfd: DurandalDeferred<any>) {
                const element = viewEngine.processMarkup(viewOrUrlOrId);
                dfd.resolve(element.cloneNode(true));
            });
        }
        
        if (viewOrUrlOrId.default && "string" === typeof viewOrUrlOrId.default && $.trim(viewOrUrlOrId.default).charAt(0) === '<') {
            return system.defer(function(dfd: DurandalDeferred<any>) {
                const element = viewEngine.processMarkup(viewOrUrlOrId.default);
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

