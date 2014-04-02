import composition = require("durandal/composition");

/*
 * A custom Knockout binding handler transforms the target element (usually a <pre>) into a code editor, powered by Ace. http://ace.c9.io
 * Usage: data-bind="aceEditor: { code: someObservableString, lang: 'ace/mode/csharp', theme: 'ace/theme/github', fontSize: '16px' }"
 * All params are optional, except code.
 */
class bsTooltipBindingHandler {


    static install() {
        if (!ko.bindingHandlers["bsTooltip"]) {
            ko.bindingHandlers["bsTooltip"] = new bsTooltipBindingHandler();

            // This tells Durandal to fire this binding handler only after composition 
            // is complete and attached to the DOM.
            // See http://durandaljs.com/documentation/Interacting-with-the-DOM/
            composition.addBindingHandler("bsTooltip");
        }
    }

    // Called by Knockout a single time when the binding handler is setup.
    init(element: HTMLElement, valueAccessor: () => { trigger?: string; placement?: string ;delay:{show?:number;hide?:number}}, allBindings, viewModel, bindingContext: any) {
        var bindingValues = valueAccessor();
        var tooltipOptions = new Object();

        tooltipOptions["trigger"] = bindingValues.trigger ? bindingValues.trigger : "hover";


        tooltipOptions["placement"] = bindingValues.placement ? bindingValues.placement : "top";

        tooltipOptions["delay"] = bindingValues.delay ? bindingValues.delay : { show: 100, hide: 100};


        $(element).tooltip(tooltipOptions);
        
    }

    // Called by Knockout each time the dependent observable value changes.
    update(element: HTMLElement, valueAccessor: () => { trigger?: string; placement?: string }, allBindings, viewModel, bindingContext: any) {
        var bindingValues = valueAccessor();
        
    }
}

export = bsTooltipBindingHandler;