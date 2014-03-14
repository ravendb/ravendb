import composition = require("durandal/composition");
import bootstrap = require("bootstrap");

class popoverBindingHandler{

    static install() {
        if (!ko.bindingHandlers["popover"]) {
            ko.bindingHandlers["popover"] = new popoverBindingHandler();

            // This tells Durandal to fire this binding handler only after composition 
            // is complete and attached to the DOM.
            // This is required so that we know the correct height for the element.
            // See http://durandaljs.com/documentation/Interacting-with-the-DOM/
            composition.addBindingHandler("popover");
        }
    }
    
    /* valueAccessor: () => { html: boolean; trigger: string; container:string; content:string;}*/
    // Called by Knockout a single time when the binding handler is setup.
    init(element: HTMLElement, valueAccessor, allBindings, viewModel, bindingContext: any) {
        var bindingValues = valueAccessor;
        var options = {
            html: bindingValues.html,
            trigger: bindingValues.trigger,
            container: bindingValues.container,
            contenr: bindingValues.content
        };
        $(element).popover(options);
    }
}

export = popoverBindingHandler;