import { CompositionContext } from "durandal/composition";
import viewEngine from "durandal/viewEngine";

const system = require("durandal/system");
const composition = require("durandal/composition");

export function overrideComposition() {
    const compose = composition.compose;
    composition.compose = function(element: HTMLElement, settings: CompositionContext) {
        if (settings.model && "string" !== typeof settings.model) {
            settings.model = system.resolveObject(settings.model);
        }

        return compose.apply(this, arguments);
    };
    
    const getSettings = composition.getSettings;
    
    composition.getSettings = function(valueAccessor: () => any, element: HTMLElement) {
        const value = valueAccessor();
        let settings = ko.utils.unwrapObservable(value) || {};
        
        if (settings.default && system.isString(settings.default)) {
            if (viewEngine.isViewUrl(settings.default)) {
                settings = {
                    view: settings.default
                };

                return settings;
            }
        }
        
        return getSettings(valueAccessor, element);
    }
}

