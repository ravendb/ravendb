define(["require", "exports", "durandal/composition"], function(require, exports, composition) {
    /*
    * A custom Knockout binding handler that causes a DOM element to change its height so that its bottom reaches to the <footer>.
    * Usage: data-bind="stickToFooter: window.ravenStudioWindowHeight()"
    */
    var stickToFooterBindingHandler = (function () {
        function stickToFooterBindingHandler() {
            var _this = this;
            var $window = $(window);
            this.windowHeightObservable = ko.observable($window.height());
            window['ravenStudioWindowHeight'] = this.windowHeightObservable.throttle(200);
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
            valueAccessor(); // Necessary to register knockout dependency. Without it, update won't fire when window height changes.
            this.stickToFooter(element);
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
                    ko.postbox.publish("StickyFooterHeightSet", { element: element, height: desiredElementHeight });
                }
            }
        };
        return stickToFooterBindingHandler;
    })();

    
    return stickToFooterBindingHandler;
});
//# sourceMappingURL=stickToFooterBindingHandler.js.map
