/// <reference path="../../typings/tsd.d.ts"/>

import virtualGrid = require("widgets/virtualGrid/virtualGrid");

class extensions {
    static install() {
        extensions.installObservableExtensions();
        extensions.installStorageExtension();
        extensions.installBindingHandlers();
        extensions.configureValidation();

        virtualGrid.install();

        // Want Intellisense for your extensions?
        // Go to extensionInterfaces.ts and add the function signature there.
    }


    private static validateDatabaseName(databaseName: string): string {
        if (!databaseName) {
            return null;
        }

        const regex1 = /^[^\\/:\*\?"<>\|]*$/; // forbidden characters \ / : * ? " < > |
        if (!regex1.test(databaseName)) {
            return `The database name can't contain any of the following characters: \\ / : * ? " < > |`;
        }

        const regex2 = /^(nul|null|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names
        if (regex2.test(databaseName)) {
            return "`The name is forbidden for use!";
        }

        if (databaseName.startsWith(".")) {
            return "The database name can't start with a dot!";
        }

        if (databaseName.endsWith(".")) {
            return "The database name can't end with a dot!";
        }

        return null;
    }

    private static configureValidation() {

        //Validate that url is in the following format: http(s)://hostName:portNumber (e.g. http://localhost:8081)
        (ko.validation.rules as any)['validUrl'] = {
            validator: (url: string) => {
                const urlRegex = /^(https?:\/\/)([^\s]+)\:([0-9]{1,5})$/; // allow any char, exclude white space
                return (urlRegex.test(url));
            },
            message: "Url format expected: 'http(s)://hostName:portNumber'"
        };  

        (ko.validation.rules as any)['validDatabaseName'] = {
            validator: (val: string) => !extensions.validateDatabaseName(val),
            message: (params: any, databaseName: KnockoutObservable<string>) => {
                return extensions.validateDatabaseName(databaseName());
            }
        };

        (ko.validation.rules as any)['base64'] = {
            validator: (val: string) => {
                const base64regex = /^([0-9a-zA-Z+/]{4})*(([0-9a-zA-Z+/]{2}==)|([0-9a-zA-Z+/]{3}=))?$/;
                return !val || base64regex.test(val);
            },
            message: 'Invaild base64 string.'
        };       

        (ko.validation.rules as any)['validJson'] = {
            validator: (text: string) => {
                let isValidJson = false;
                try {
                    JSON.parse(text);
                    isValidJson = true;
                }
                catch (e) { }
                return isValidJson;
            },
            message: 'Invalid json format.'
        };

        (ko.validation.rules as any)['validJavascript'] = {
            validator: (text: string) => {
                try {
                    eval("throw 0;" + text);
                } catch (e) {
                    if (e === 0)
                        return true;
                }
                return false;
            },
            message: 'Invalid javascript.'
        };

        ko.validation.init({
            errorElementClass: 'has-error',
            errorMessageClass: 'help-block',
            decorateInputElement: true
        });
    }

    private static installObservableExtensions() {
        const subscribableFn: any = ko.subscribable.fn;

        subscribableFn.distinctUntilChanged = function () {
            const observable: KnockoutObservable<any> = this;
            const matches = ko.observable();
            let lastMatch = observable();
            observable.subscribe(val => {
                if (val !== lastMatch) {
                    lastMatch = val;
                    matches(val);
                }
            });
            return matches;
        };

        subscribableFn.throttle = function (throttleTimeMs: number) {
            const observable = this;
            return ko.pureComputed(() => observable()).extend({ throttle: throttleTimeMs });
        };

        subscribableFn.toggle = function () {
            const observable: KnockoutObservable<boolean> = this;
            observable(!observable());
            return observable;
        };
    }

    private static installStorageExtension() {
        Storage.prototype.getObject = function (key) {
            const value = this.getItem(key);
            return value && JSON.parse(value);
        }

        Storage.prototype.setObject = function (key, value) {
            this.setItem(key, ko.toJSON(value));
        }
    }

    private static interceptModalsInDropdownPanels(element: HTMLElement) {
        const $toggle = $(element).parent();

        const onModalClicked = (e: JQueryEventObject) => {
            // both dropdown and modal are visible, don't hide dropdown when backdrop or modal clicked
            e.stopPropagation();
        }

        const onModalShown = () => {
            $(".modal").click(onModalClicked);
        };

        $toggle.on('shown.bs.dropdown', () => {
            $(document).on("shown.bs.modal", onModalShown);
        });
        $toggle.on('hidden.bs.dropdown', () => {
            $(document).off("shown.bs.modal", onModalShown);
        });
    }

    private static installBindingHandlers() {
      
        ko.bindingHandlers["scrollTo"] = {
            update: (element: any, valueAccessor: KnockoutObservable<boolean>) => {
                if (valueAccessor()) {
                  
                    const $container = $("#page-host-root");
                    const scrollTop = $container.scrollTop();
                    const scrollBottom = scrollTop + $container.height();

                    const $element = $(element);
                    const elementTop = $element.position().top;
                    const elementBottom = elementTop + $element.height();

                    // Scroll vertically only if element is outside of viewport 
                    if ((elementTop < scrollTop) || (elementBottom > scrollBottom)){
                        $container.scrollTop(elementTop);
                    } 
                }
            }
        };

        ko.bindingHandlers["collapse"] = {
            init: (element: any, valueAccessor: () => KnockoutObservable<boolean>) => {
                const value = valueAccessor();
                const valueUnwrapped = ko.unwrap(value);
                const $element = $(element);
                $element
                    .addClass('collapse')
                    .collapse({
                        toggle: valueUnwrapped
                    });

                // mark element is being initialized to allow initial animation to take place
                $(element).data('bs.collapse').initializing = true;
            },

            update: (element: any, valueAccessor: () => KnockoutObservable<boolean>) => {
                const value = valueAccessor();
                const valueUnwrapped = ko.unwrap(value);
                const bsData = $(element).data('bs.collapse');

                const action = valueUnwrapped ? "show" : "hide";

                const isInit = bsData.initializing;

                if (isInit) {
                    delete bsData.initializing;
                } else {
                    const transitioning = bsData.transitioning;

                    if (!transitioning) {
                        // if there isn't any other animation in progress - proceed
                        $(element).collapse(action);
                    } else if (ko.isObservable(value)) {
                        // have we another animation in progress - try to recover this state by reseting checkbox
                        value(!value());
                    }
                }
            }
        };

        ko.bindingHandlers["numericValue"] = {
            init: (element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) => {
                const underlyingObservable = valueAccessor();
                const interceptor = ko.pureComputed({
                    read: underlyingObservable,
                    write: (value: any) => {
                        if (value && !isNaN(value)) {
                            underlyingObservable(parseFloat(value));
                        } else {
                            underlyingObservable(undefined);
                        }
                    },
                    disposeWhenNodeIsRemoved: element
                });

                // copy validation 
                interceptor.rules = underlyingObservable.rules;
                interceptor.isValid = underlyingObservable.isValid;
                interceptor.isModified = underlyingObservable.isModified;
                interceptor.error = underlyingObservable.error;

                ko.bindingHandlers.value.init(element, () => interceptor, allBindingsAccessor, viewModel, bindingContext);
            },
            update: ko.bindingHandlers.value.update
        };

        ko.bindingHandlers["numericInput"] = {
            init: (element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) => {
                const underlyingObservable = valueAccessor();
                const interceptor = ko.pureComputed({
                    read: underlyingObservable,
                    write: (value: any) => {
                        if (value && !isNaN(value)) {
                            underlyingObservable(parseFloat(value));
                        } else {
                            underlyingObservable(undefined);
                        }
                    },
                    disposeWhenNodeIsRemoved: element
                });

                // copy validation 
                interceptor.rules = underlyingObservable.rules;
                interceptor.isValid = underlyingObservable.isValid;
                interceptor.isModified = underlyingObservable.isModified;
                interceptor.error = underlyingObservable.error;
                
                ko.bindingHandlers.textInput.init(element, () => interceptor, allBindingsAccessor, viewModel, bindingContext);
            },
            update: ko.bindingHandlers.textInput.update
        };

        ko.bindingHandlers["dropdownPanel"] = {
            init: (element) => {
                extensions.interceptModalsInDropdownPanels(element);

                $(element).on('click', e => {
                    const $target = $(e.target);

                    const clickedOnClose = !!$target.closest(".close-panel").length;
                    if (clickedOnClose) {
                        const $dropdownParent = $target.closest(".dropdown-menu").parent();
                        $dropdownParent.removeClass('open');
                    } else {
                        const $button = $target.closest(".dropdown-toggle");
                        const $dropdown = $button.next(".dropdown-menu");
                        if ($dropdown.length && $dropdown[0] !== element) {
                            if (!$button.is(":disabled")) {
                                const $parent = $dropdown.parent();
                                $parent.toggleClass('open');
                            }
                        } else {
                            // close any child dropdown
                            $(".dropdown", element).each((idx, elem) => {
                                $(elem).removeClass('open');
                            });
                        }

                        e.stopPropagation();
                    }
                });
            }
        }

        ko.bindingHandlers["checkboxTriple"] = {
            update(element, valueAccessor, allBindings, viewModel, bindingContext) {
                const checkboxValue: checkbox = ko.unwrap(valueAccessor());
                switch (checkboxValue) {
                case checkbox.Checked:
                    element.checked = true;
                    element.readOnly = false;
                    element.indeterminate = false;
                    break;
                case checkbox.SomeChecked:
                    element.readOnly = true;
                    element.indeterminate = true;
                    element.checked = false;
                    break;
                case checkbox.UnChecked:
                    element.checked = false;
                    element.readOnly = false;
                    element.indeterminate = false;
                    break;
                }
            }
        };

        ko.bindingHandlers["durationPicker"] = {
            init: (element, valueAccessor: () => KnockoutObservable<number>, allBindings) => {
                const $element = $(element);
                const bindings = allBindings();

                const showDays = bindings.durationPickerOptions ? bindings.durationPickerOptions.showDays : false;
                const showSeconds = bindings.durationPickerOptions ? bindings.durationPickerOptions.showSeconds : false;

                $element.durationPicker({
                    showDays: showDays,
                    showSeconds: showSeconds,
                    onChanged: (value: string, isInit: boolean) => {
                        if (!isInit) {
                            const underlyingObservable = valueAccessor();
                            underlyingObservable(parseInt(value, 10));
                        }
                    }
                });
                const value = ko.unwrap(valueAccessor());
                $(element).data('durationPicker').setValue(value);

                ko.utils.domNodeDisposal.addDisposeCallback(element, () => {
                    $element.data('durationPicker').destroy();
                });
            },
            update: (element, valueAccessor) => {
                const value = ko.unwrap(valueAccessor());
                $(element).data('durationPicker').setValue(value);
            }
        }
    }

}

export = extensions;
