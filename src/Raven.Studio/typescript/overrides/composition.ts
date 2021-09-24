import { CompositionContext } from "durandal/composition";

const system = require("durandal/system");
const composition = require("durandal/composition");

export function overrideComposition() {
    const compose = composition.compose;
    composition.compose = function(element: HTMLElement, settings: CompositionContext) {
        if ("string" !== typeof settings.model) {
            settings.model = system.resolveObject(settings.model);
        }

        return compose.apply(this, arguments);
    };
}

