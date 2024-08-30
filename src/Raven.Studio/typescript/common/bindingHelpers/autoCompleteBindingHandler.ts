import composition = require("durandal/composition");

/*
 * A KnockoutJS binding handler that transforms a div into the auto-complete items container for a text box.
 *
 * Usage for auto complete: 
 *           <input id="myAutoComplete" type="text" data-bind="textInput: mySearchValue" />
 *           <div style="display: none" data-bind="autoComplete: '#myAutoComplete', foreach: yourOwnResults">
 *               <div class="text" data-bind="text: name"></div>
 *           </div>
 *
 * Usage for auto complete without selecting the scrolled down/up field: 
 *           <input id="myAutoComplete" type="text" data-bind="value: mySearchValue, valueUpdate: 'afterkeydown'" />
 *           <div style="display: none" data-bind="autoComplete: '#myAutoComplete', foreach: yourOwnResults">
 *               <div data-bind="text: name"></div>
 *           </div>
 *           
 * In the above sample, yourOwnResults is an array that you are responsible for populating. And 'name' is the property on the items in that array.
 */
class autoCompleteBindingHandler {

    static install() {
        if (!ko.bindingHandlers["autoComplete"]) {
            ko.bindingHandlers["autoComplete"] = new autoCompleteBindingHandler();

            // This tells Durandal to fire this binding handler only after composition 
            // is complete and attached to the DOM.
            // See http://durandaljs.com/documentation/Interacting-with-the-DOM/
            composition.addBindingHandler("autoComplete");
        }
    }

    // Called by Knockout a single time when the binding handler is setup.
    init(element: HTMLElement, valueAccessor: () => string, allBindings: () => any) {
        const inputId = valueAccessor();
        const input = $(inputId);
        if (input.length !== 1) {
            // Don't throw an error here, because we may cancel navigation, and Durandal may pull the element out.
            // Instead, we'll just issue a warning in the console and return.
            //throw new Error("Expected 1 auto complete element, '" + inputId + "', but found " + input.length);
            console.warn("Expected 1 auto complete element, '" + inputId + "', but found " + input.length);
            return;
        }

        // Hide the auto complete container and size it to the same size as the textbox.
        const $element = $(element);
        element.style.display = "none";
        element.style.position = "absolute";
        element.style.left = "auto";
        element.style.top = (input.height() + 20) + "px";

        //This makes elements with long names overflow the container... commenting it for the moment
        //element.style.width = input.width() + "px";

        // Clicking an element in the auto complete list should hide it.
        $element.on('click', () => setTimeout(() => element.style.display = "none", 0));

        // Leaving the textbox should hide the auto complete list.
        input.on('blur', () => setTimeout(() => {
            element.style.display = "none";

            const newValue = $element.find(".active").find("span.text").text();
            if (newValue.length === 0) {
                return;
            }

            const oldValue = input.text();
            if (oldValue === newValue) {
                return;
            }

            input.text(newValue);
            input.trigger("change");
        }, 200));

        // Putting the focus back on the textbox should show the auto complete list if we have items.
        input.on('focus', () => setTimeout(() =>
            element.style.display = this.getAllAutoCompleteItems($element).length > 0 ? "block" : "none"));

        // Up, down, enter all have special meaning.
        input.on('keydown', (args) => this.handleKeyPress(element, $element, input, args));

        // When the results change and we have 1 or more, display the auto complete container.
        const results: KnockoutObservableArray<any> = allBindings()['foreach'];
        if (!results) {
            throw new Error("Unable to find results list for auto complete.");
        }
        const subscription = results.subscribe((array: any[]) => {
            element.style.display = array.length === 0 || !input.is(":focus") ? "none" : "block";
        });

        // Clean up after ourselves when the node is removed from the DOM.
        ko.utils.domNodeDisposal.addDisposeCallback(element, () => {
            input.off('blur');
            $element.off('click');
            input.off('keydown');
            subscription.dispose();
        });
    }

    getAllAutoCompleteItems(resultContainer: JQuery): JQuery {
        return resultContainer.children("li");
    }

    findAutoCompleteItemMatching(resultContainer: JQuery, text: string): HTMLElement {
        const textLower = text.toLowerCase();
        return this.getAllAutoCompleteItems(resultContainer)
            .toArray()
            .filter((el: HTMLElement) => el.textContent && el.textContent.trim().toLowerCase().indexOf(textLower) >= 0)[0];
    }

    handleKeyPress(element: HTMLElement, $element: JQuery, $input: JQuery<HTMLElement>, args: JQuery.KeyDownEvent<HTMLElement>) {
        const enter = 13;
        const escape = 27;
        const downArrow = 40;
        const upArrow = 38;

        if (args.which === escape) {
            element.style.display = "none";
        }

        let lis: JQuery, curSelected: JQuery, selected: JQuery;
        if (element.style.display == "none" && args.which === downArrow) {
            if ($element.children("li").length > 0 && $input.is(":focus")) {
                setTimeout(() => element.style.display = "block", 0);
                return true;
            }
        }

        if (args.which === downArrow || args.which === upArrow || args.which === enter) {
            lis = this.getAllAutoCompleteItems($element);
            curSelected = $element.find(".active");
        }

        if (args.which === downArrow) {
            if (curSelected.length > 0) {
                curSelected.removeClass("active");
                const nextSelected = curSelected.next();

                if (nextSelected.length) {
                    selected = nextSelected;
                    nextSelected.addClass("active");
                    $element.scrollTop((nextSelected.index() - 1) * 30);
                } else {
                    selected = lis.first();
                    selected.addClass("active");
                    $element.scrollTop(0);
                }

            } else {
                selected = lis.first();
                curSelected = selected.addClass("active");
            }

            this.updateInputElement(selected, $input);
        } else if (args.which === upArrow) {
            args.preventDefault();
            if (curSelected.length > 0) {
                curSelected.removeClass("active");
                const prevSelected = curSelected.prev();

                if (prevSelected.length) {
                    selected = prevSelected;
                    prevSelected.addClass("active");
                    $element.scrollTop((prevSelected.index() - 1) * 30);
                } else {
                    selected = lis.last();
                    selected.addClass("active");
                    $element.scrollTop($element.children("li").length * 30);
                }

            } else {
                selected = lis.first();
                curSelected = selected.addClass("active");
            }

            this.updateInputElement(selected, $input);
        } else if (args.which === enter) {
            args.preventDefault();
            args.stopPropagation();
            const itemToSelect = curSelected.length ? curSelected : $(this.findAutoCompleteItemMatching($element, $input.val() as string));
            if (itemToSelect.length) {
                itemToSelect.click();
            }
        }
    }

    private updateInputElement(selected: JQuery, $input: JQuery) {
        const selectedValue = selected.find("span.text");
        if (selectedValue.length === 0) {
            return;
        }

        $input.val(selectedValue.text());
        const htmlElement: HTMLElement = $input[0];
        htmlElement.scrollLeft = htmlElement.scrollWidth;
    }
}

export = autoCompleteBindingHandler;
