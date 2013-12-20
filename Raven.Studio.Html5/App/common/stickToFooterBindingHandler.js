define(["require", "exports", "durandal/composition"], function(require, exports, composition) {
    /*
    * A custom Knockout binding handler that causes a DOM element to change its height so that its bottom reaches to the <footer>.
    * Usage: data-bind="stickToFooter: window.ravenStudioWindowHeight()"
    */
    var stickToFooterBindingHandler = (function () {
        function stickToFooterBindingHandler() {
            var _this = this;
            this.throttleTimeMs = 100;
            var $window = $(window);
            this.windowHeightObservable = ko.observable($window.height());
            window['ravenStudioWindowHeight'] = this.windowHeightObservable.throttle(this.throttleTimeMs);
            $window.resize(function (ev) {
                return _this.windowHeightObservable($window.height());
            });
        }
        stickToFooterBindingHandler.prototype.install = function () {
            ko.bindingHandlers['stickToFooter'] = this;

            // This tells Durandal to fire this binding handler only after composition
            // is complete and attached to the DOM.
            // This is required so that we know the correct height for the element.
            // See http://durandaljs.com/documentation/Interacting-with-the-DOM/
            composition.addBindingHandler('stickToFooter');
        };

        // Called by Knockout a single time when the binding handler is setup.
        stickToFooterBindingHandler.prototype.init = function (element, valueAccessor, allBindings, viewModel, bindingContext) {
            element.style.overflowY = "auto";
            element.style.overflowX = "hidden";
        };

        // Called by Knockout each time the dependent observable value changes.
        stickToFooterBindingHandler.prototype.update = function (element, valueAccessor, allBindings, viewModel, bindingContext) {
            var newWindowHeight = valueAccessor();

            // Check what was the last dispatched height to this element.
            var lastWindowHeightKey = "ravenStudioLastDispatchedHeight";
            var lastWindowHeight = ko.utils.domData.get(element, lastWindowHeightKey);
            if (lastWindowHeight !== newWindowHeight) {
                ko.utils.domData.set(element, lastWindowHeightKey, newWindowHeight);
                this.stickToFooter(element);
            }
        };

        stickToFooterBindingHandler.prototype.stickToFooter = function (element) {
            var $element = $(element);
            var isVisible = $element.is(":visible");
            if (isVisible) {
                var elementTop = $element.offset().top;
                var footerTop = $("footer").position().top;
                var padding = 5;
                var desiredElementHeight = footerTop - elementTop - padding;
                var minimumHeight = 100;
                if (desiredElementHeight >= minimumHeight) {
                    $element.height(desiredElementHeight);
                    $element.trigger("StickyFooterHeightSet", desiredElementHeight);
                }
            }
        };
        return stickToFooterBindingHandler;
    })();

    
    return stickToFooterBindingHandler;
});
//# sourceMappingURL=stickToFooterBindingHandler.js.map
