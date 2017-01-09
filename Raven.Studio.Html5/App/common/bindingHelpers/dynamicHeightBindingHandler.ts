import composition = require("durandal/composition");

/*
 * A custom Knockout binding handler that causes a DOM element to change its height so that its bottom reaches to a target element.
 * Usage: data-bind="dynamicHeight: { resizeTrigger: window.ravenStudioWindowHeight(), target: 'footer' }"
 * Target can be whatever element you choose.
 */
class dynamicHeightBindingHandler {
    windowHeightObservable: KnockoutObservable<number>;
    throttleTimeMs = 100;

    static updateForced = ko.observable<number>();

    constructor() {
        var $window = $(window);
        this.windowHeightObservable = ko.observable<number>($window.height());
        window['ravenStudioWindowHeight'] = this.windowHeightObservable.throttle(this.throttleTimeMs);
        $window.resize((ev: JQueryEventObject) => this.windowHeightObservable($window.height()));
    }

    static install() {
        if (!ko.bindingHandlers["dynamicHeight"]) {
            ko.bindingHandlers["dynamicHeight"] = new dynamicHeightBindingHandler();

            // This tells Durandal to fire this binding handler only after composition 
            // is complete and attached to the DOM.
            // This is required so that we know the correct height for the element.
            // See http://durandaljs.com/documentation/Interacting-with-the-DOM/
            composition.addBindingHandler('dynamicHeight');
        }
    }

    static forceUpdate() {
        dynamicHeightBindingHandler.updateForced(new Date().getTime());
    }

    // Called by Knockout a single time when the binding handler is setup.
    init(element: HTMLElement, valueAccessor: () => { resizeTrigger: number; target?: string; bottomMargin: number; container?: string }, allBindings: any, viewModel: any, bindingContext: KnockoutBindingContext) {

        var bindingValue = valueAccessor();
        if (bindingValue.target) {
            var targetSelector = bindingValue.target || "footer";
            var bottomMargin = ko.unwrap(bindingValue.bottomMargin) || 0;
            var container = bindingValue.container;

            var forcedUpdateSubscription = dynamicHeightBindingHandler.updateForced.subscribe(() => {
                dynamicHeightBindingHandler.stickToTarget(element, targetSelector, bottomMargin, container);
            });

            ko.utils.domNodeDisposal.addDisposeCallback(element, () => {
                forcedUpdateSubscription.dispose();
            });
        }
        
    }

    // Called by Knockout each time the dependent observable value changes.
    update(element: HTMLElement, valueAccessor: () => { resizeTrigger: number; target?: string; bottomMargin: number; container?: string }, allBindings: any, viewModel: any, bindingContext: KnockoutBindingContext) {
        var bindingValue = valueAccessor(); // Necessary to register knockout dependency. Without it, update won't fire when window height changes.
        if (bindingValue.target) {

            var newWindowHeight = bindingValue.resizeTrigger;
            var targetSelector = bindingValue.target || "footer";
            var bottomMargin = ko.unwrap(bindingValue.bottomMargin) || 0;
            var container = bindingValue.container;

            // Check what was the last dispatched height to this element.
            var lastWindowHeightKey = "ravenStudioLastDispatchedHeight";
            var lastWindowHeight: number = ko.utils.domData.get(element, lastWindowHeightKey);
            if (lastWindowHeight !== newWindowHeight) {
                ko.utils.domData.set(element, lastWindowHeightKey, newWindowHeight);
                dynamicHeightBindingHandler.stickToTarget(element, targetSelector, bottomMargin, container);
            }
        }
    }

    static stickToTarget(element: HTMLElement, targetSelector: string, bottomMargin: number, container?: string) {
        var targetElement = $(targetSelector);
        if (targetSelector.length === 0) {
            throw new Error("Couldn't configure dynamic height because the target element isn't on the page. Target element: " + targetSelector);
        }

        var $element = $(element);
        var isVisible = $element.is(":visible");

        if (isVisible) {

            var containerOffset = 0;

            if (container) {
                var $container = $(container);
                var topOffsetDiff = $element.offset().top - $container.offset().top;
                containerOffset = $container.outerHeight() - topOffsetDiff - $element.outerHeight();
            }

            var elementTop = $element.offset().top;
            var footerTop = targetElement.position().top;
            var padding = 5 + bottomMargin;
            var desiredElementHeight = footerTop - elementTop - padding - containerOffset;
            if ($(document).fullScreen()) {
                var windowHeightKey = "ravenStudioLastDispatchedHeight";
                var windowHeight: number = ko.utils.domData.get(element, windowHeightKey);
                desiredElementHeight = windowHeight - elementTop - padding;
            }
            $element.innerHeight(desiredElementHeight);
            $element.trigger("DynamicHeightSet", desiredElementHeight);
        }
    }
}

export = dynamicHeightBindingHandler;
