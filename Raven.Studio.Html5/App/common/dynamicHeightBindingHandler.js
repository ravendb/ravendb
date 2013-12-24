define(["require", "exports", "durandal/composition"], function(require, exports, composition) {
    /*
    * A custom Knockout binding handler that causes a DOM element to change its height so that its bottom reaches to a target element.
    * Usage: data-bind="dynamicHeight: { resizeTrigger: window.ravenStudioWindowHeight(), target: 'footer' }"
    * Target can be whatever element you choose.
    */
    var dynamicHeightBindingHandler = (function () {
        function dynamicHeightBindingHandler() {
            var _this = this;
            this.throttleTimeMs = 100;
            var $window = $(window);
            this.windowHeightObservable = ko.observable($window.height());
            window['ravenStudioWindowHeight'] = this.windowHeightObservable.throttle(this.throttleTimeMs);
            $window.resize(function (ev) {
                return _this.windowHeightObservable($window.height());
            });
        }
        dynamicHeightBindingHandler.prototype.install = function () {
            ko.bindingHandlers['dynamicHeight'] = this;

            // This tells Durandal to fire this binding handler only after composition
            // is complete and attached to the DOM.
            // This is required so that we know the correct height for the element.
            // See http://durandaljs.com/documentation/Interacting-with-the-DOM/
            composition.addBindingHandler('dynamicHeight');
        };

        // Called by Knockout a single time when the binding handler is setup.
        dynamicHeightBindingHandler.prototype.init = function (element, valueAccessor, allBindings, viewModel, bindingContext) {
            element.style.overflowY = "auto";
            element.style.overflowX = "hidden";
        };

        // Called by Knockout each time the dependent observable value changes.
        dynamicHeightBindingHandler.prototype.update = function (element, valueAccessor, allBindings, viewModel, bindingContext) {
            var bindingValue = valueAccessor();
            var newWindowHeight = bindingValue.resizeTrigger;
            var targetSelector = bindingValue.target || "footer";

            // Check what was the last dispatched height to this element.
            var lastWindowHeightKey = "ravenStudioLastDispatchedHeight";
            var lastWindowHeight = ko.utils.domData.get(element, lastWindowHeightKey);
            if (lastWindowHeight !== newWindowHeight) {
                ko.utils.domData.set(element, lastWindowHeightKey, newWindowHeight);
                this.stickToTarget(element, targetSelector);
            }
        };

        dynamicHeightBindingHandler.prototype.stickToTarget = function (element, targetSelector) {
            var targetElement = $(targetSelector);
            if (targetSelector.length === 0) {
                throw new Error("Couldn't configure dynamic height because the target element isn't on the page. Target element: " + targetSelector);
            }

            var $element = $(element);
            var isVisible = $element.is(":visible");

            if (isVisible) {
                var elementTop = $element.offset().top;
                var footerTop = $(targetSelector).position().top;
                var padding = 5;
                var desiredElementHeight = footerTop - elementTop - padding;
                var minimumHeight = 100;
                if (desiredElementHeight >= minimumHeight) {
                    $element.height(desiredElementHeight);
                    $element.trigger("DynamicHeightSet", desiredElementHeight);
                }
            }
        };
        return dynamicHeightBindingHandler;
    })();

    
    return dynamicHeightBindingHandler;
});
//# sourceMappingURL=dynamicHeightBindingHandler.js.map
