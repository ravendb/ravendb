/// <reference path="../../../typings/tsd.d.ts" />

import composition = require("durandal/composition");

/*
 * A custom Knockout binding handler that causes the collections labels window to become resizable
 * Usage: data-bind="enableResize: true"
 */
class enableResizeBindingHandler {
    static install() {
        if (!ko.bindingHandlers["enableResize"]) {
            ko.bindingHandlers["enableResize"] = new enableResizeBindingHandler();

            // This tells Durandal to fire this binding handler only after composition 
            // is complete and attached to the DOM.
            // This is required so that we know the correct height for the element.
            // See http://durandaljs.com/documentation/Interacting-with-the-DOM/
            composition.addBindingHandler('enableResize');
        }
    }

    // Called by Knockout a single time when the binding handler is setup.
    init(element: HTMLElement, valueAccessor: () => string, allBindings: () => any, viewModel: any, bindingContext: any) {
        /* TODO:
        var options: any = {
            handles: 'e, w',
            helper: string => "resizable-helper",
            resize: (event: Event, ui: JQueryUI.ResizableUIParams) => {
                $(element).find('.col-resizable-target').width(ui.size.width - 40);
                $(element).find('.col-resizable-affected').width($(element).width() - ui.size.width - 30);
            }
        };

        // initialize the resizable control
        $(element).find('.col-resizable-target').resizable(options);*/
    }
}

export = enableResizeBindingHandler;
