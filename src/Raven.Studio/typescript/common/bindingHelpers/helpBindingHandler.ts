import composition = require("durandal/composition");
import viewModelBase = require("viewmodels/viewModelBase");

class helpBindingHandler {

    link: JQuery;

    static install() {
        if (!ko.bindingHandlers["help"]) {
            ko.bindingHandlers["help"] = new helpBindingHandler();

            // This tells Durandal to fire this binding handler only after composition 
            // is complete and attached to the DOM.
            // This is required so that we know the correct height for the element.
            // See http://durandaljs.com/documentation/Interacting-with-the-DOM/
            composition.addBindingHandler('help');
        }
    }

    // Called by Knockout a single time when the binding handler is setup.
    init(element: HTMLElement, valueAccessor: () =>  any, allBindings: any, viewModel: viewModelBase, bindingContext: KnockoutBindingContext) {
        this.link = $('<a class="help_link"><i class="fa fa-question-circle"></i></a>').attr('target', '_blank');
        $(element).append(this.link);
    }

    update(element: HTMLInputElement, valueAccessor: () => any, allBindingsAccessor: KnockoutAllBindingsAccessor, viewModel: viewModelBase, bindingContext: KnockoutBindingContext) {
        var value = valueAccessor();
        var hashUnwrapped = ko.unwrap(value.hash);
        var version = viewModelBase.clientVersion();
        var titleUnwrapped = ko.unwrap(value.title);
        var href = "http://ravendb.net/l/" + hashUnwrapped + "/" + version + "/";
        this.link.attr('href', href);
        this.link.attr('title', titleUnwrapped);
    }
}

export = helpBindingHandler;
