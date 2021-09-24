const $ = require('jquery');
const system = require('durandal/system');
const viewLocator = require('durandal/viewLocator');
const viewEngine = require("durandal/viewEngine");

export function overrideViews() {
// Allow using `function` or bare HTML string as a view
    const locateView = viewLocator.locateView;
    viewLocator.locateView = function(viewOrUrlOrId:string, area: string) {
        // HTML here will be passed into `processMarkup`
        if ("string" === typeof viewOrUrlOrId && $.trim(viewOrUrlOrId).charAt(0) === '<') {
            return system.defer(function(dfd: DurandalDeferred<any>) {
                const element = viewEngine.processMarkup(viewOrUrlOrId);
                dfd.resolve(element.cloneNode(true));
            });
        }

        // super()
        return locateView.apply(this, arguments);
    };


    const isViewUrl = viewEngine.isViewUrl;

    viewEngine.isViewUrl = function(url: string) {
        //TODO:
        if (url && url.startsWith("<")) {
            return true;
        }

        return isViewUrl(url);
    };
}

