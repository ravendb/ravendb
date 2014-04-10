import composition = require("durandal/composition");

class bootstrapPopoverBindingHandler {

    defaults = {
        placement: "right",
        title: "",
        html: true,
        content: "",
        trigger: "manual"
    }

    static install() {
        if (!ko.bindingHandlers.bootstrapPopover) {
            ko.bindingHandlers.bootstrapPopover = new bootstrapPopoverBindingHandler();

            // This tells Durandal to fire this binding handler only after composition 
            // is complete and attached to the DOM.
            // See http://durandaljs.com/documentation/Interacting-with-the-DOM/
            composition.addBindingHandler("bootstrapPopover");
        }
    }

    init(element: HTMLElement, valueAccessor, allBindingsAccessor, viewModel, bindingContext: any) {

        // read popover options
        var popoverBindingValues = ko.unwrap(valueAccessor());

        // set popover title
        var popoverTitle = popoverBindingValues.title;

        // set popover template id
        var tmplId = popoverBindingValues.template;

        // set data for template
        var data = popoverBindingValues.data;

        // set popover trigger
        var trigger = 'click';

        // set event type for binding bind
        var eventType = 'click';

        if (popoverBindingValues.trigger) {
            trigger = popoverBindingValues.trigger;
        }

        // update triggers
        if (trigger === 'hover') {
            eventType = 'mouseenter mouseleave';
        } else if (trigger === 'focus') {
            eventType = 'focus blur';
        }

        // set popover placement
        var placement = popoverBindingValues.placement;
        var tmplHtml;

        // get template html
        if (!data) {
            tmplHtml = $('#' + tmplId).html();
        } else {
            tmplHtml = () => {
                var container = $('<div data-bind="template: { name: template, if: data, data: data }"></div>');

                ko.applyBindings({
                    template: tmplId,
                    data: data
                }, container[0]);
                return container;
            };
        }

        // create unique identifier to bind to
        var uuid = this.guid();
        var domId = "ko-bs-popover-" + uuid;

        // create correct binding context
        var childBindingContext = bindingContext.createChildContext(viewModel);

        // create DOM object to use for popover content
        var tmplDom = $('<div/>', {
            "class": "ko-popover",
            "id": domId
        }).html(tmplHtml);

        // set content options
        var options = (<any>{
            content: this.getOuterHtml($(tmplDom[0])),
            title: popoverTitle
        });

        if (placement) {
            options.placement = placement;
        }

        if (popoverBindingValues.container) {
            options.container = popoverBindingValues.container;
        }

        // Need to copy this, otherwise all the popups end up with the value of the last item
        var popoverOptions = $.extend({}, ko.bindingHandlers.bootstrapPopover.options, options);

        // bind popover to element click
        $(element).bind(eventType, function () {
            var popoverAction = 'show';
            var popoverTriggerEl = $(this);

            // popovers that hover should be toggled on hover
            // not stay there on mouseout
            if (trigger !== 'click') {
                popoverAction = 'toggle';
            }

            // show/toggle popover
            popoverTriggerEl.popover(popoverOptions).popover(popoverAction);

            // hide other popovers and bind knockout to the popover elements
            var popoverInnerEl = $('#' + domId);
            $('.ko-popover').not(popoverInnerEl).parents('.popover').remove();

            // if the popover is visible bind the view model to our dom ID
            if (popoverInnerEl.is(':visible')) {

                ko.applyBindingsToDescendants(childBindingContext, popoverInnerEl[0]);

                /* Since bootstrap calculates popover position before template is filled,
                 * a smaller popover height is used and it appears moved down relative to the trigger element.
                 * So we have to fix the position after the bind
                */

                var triggerElementPosition = $(element).offset().top;
                var triggerElementLeft = $(element).offset().left;
                var triggerElementHeight = $(element).outerHeight();
                var triggerElementWidth = $(element).outerWidth();

                var popover = (<any>$(popoverInnerEl)).parents('.popover');
                var popoverHeight = popover.outerHeight();
                var popoverWidth = popover.outerWidth();
                var arrowSize = 10;

                switch (popoverOptions.placement) {
                    case 'left':
                        popover.offset({ top: triggerElementPosition - popoverHeight / 2 + triggerElementHeight / 2, left: triggerElementLeft - arrowSize - popoverWidth });
                        break;
                    case 'right':
                        popover.offset({ top: triggerElementPosition - popoverHeight / 2 + triggerElementHeight / 2 });
                        break;
                    case 'top':
                        popover.offset({ top: triggerElementPosition - popoverHeight - arrowSize, left: triggerElementLeft - popoverWidth / 2 + triggerElementWidth / 2 });
                        break;
                    case 'bottom':
                        popover.offset({ top: triggerElementPosition + triggerElementHeight + arrowSize, left: triggerElementLeft - popoverWidth / 2 + triggerElementWidth / 2 });
                }
            }

            // bind close button to remove popover
            $(document).on('click', '[data-dismiss="popover"]', e => {
                popoverTriggerEl.popover('hide');
            });

            // Also tell KO *not* to bind the descendants itself, otherwise they will be bound twice
            return { controlsDescendantBindings: true };
        });
    }

    update(element: HTMLElement, valueAccessor, allBindings, viewModel, bindingContext: any) {
        
    }

    //UUID
    s4() {
        "use strict";
        return Math.floor((1 + Math.random()) * 0x10000)
            .toString(16)
            .substring(1);
    }

    guid() {
        "use strict";
        return this.s4() + this.s4() + '-' + this.s4() + '-' + this.s4() + '-' + this.s4() + '-' + this.s4() + this.s4() + this.s4();
    }

    getOuterHtml(element) {

        if (element.length === 0) {
            return false;
        }

        var elem = element[0], name = elem.tagName.toLowerCase();
        if (elem.outerHTML) {
            return elem.outerHTML;
        }
        var attrs = $.map(elem.attributes, i => i.name + '="' + i.value + '"');
        return "<" + name + (attrs.length > 0 ? " " + attrs.join(" ") : "") + ">" + elem.innerHTML + "</" + name + ">";

    }
}

export = bootstrapPopoverBindingHandler;