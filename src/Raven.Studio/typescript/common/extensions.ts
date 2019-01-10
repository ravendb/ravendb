/// <reference path="../../typings/tsd.d.ts"/>
import virtualGrid = require("widgets/virtualGrid/virtualGrid");
import listView = require("widgets/listView/listView");
import genUtils = require("common/generalUtils");

class extensions {
    static install() {
        extensions.installObservableExtensions();
        extensions.installStorageExtension();
        extensions.installBindingHandlers();
        extensions.configureValidation();

        virtualGrid.install();
        listView.install();

        // Want Intellisense for your extensions?
        // Go to extensionInterfaces.ts and add the function signature there.
    }

    private static validateUrl(url: string): string {
        if (!url) {
            return null;
        }
        
        url = _.trim(url);

        const urlRegex = /^(https?:\/\/)([^\s]+)$/; // allow any char, exclude white space
        if (!urlRegex.test(url)) {
            return "Url format expected: 'http(s)://hostName'";
        }

        try {
            new URL(url);
        } catch (e) {
            return (e as Error).message;
        }

        return null;
    }
    
    private static validateNoPort(address: string) : string {
        if (!address || address === 'localhost' || address === '::1') {  
            return null;
        }

        const addressAsArray = Array.from(address);
        const colonsCount = addressAsArray.filter(x => x === ':').length;
        const dotsCount = addressAsArray.filter(x => x === '.').length;
        
        if (dotsCount === 3 && colonsCount > 0) {
            return `Please enter IP Address without a port number`;
        }
        
        if (colonsCount === 8) {
            return `Please enter IP Address without a port number`;
        }
        
        return null;
    }
    
    private static validateAddressAndNoPort(address: string) : string {          
        if (!address || address === 'localhost' || address === '::1') {
            return null;
        }       
                
        if (!_.includes(address, '.') && !_.includes(address, ':')) {
            return "Please enter a valid IPv4 or IPv6 address";
        }   
        
        const addressWithPort = extensions.validateNoPort(address);        
        if (addressWithPort) {
            return addressWithPort;
        }
          
        if (!genUtils.regexIPv4.test(address)) {
            
            // TODO: check if this is a valid IPv6....
        }
        
        return null;
    }
     
    private static configureValidation() {

        // Validate that url is in the following format: http(s)://hostName (e.g. http://localhost)
        (ko.validation.rules as any)['validUrl'] = {
            validator: (val: string) => !extensions.validateUrl(val),
            message: (params: any, url: KnockoutObservable<string>) => {
                return extensions.validateUrl(url());
            }
        };

        (ko.validation.rules as any)['noPort'] = {
            validator: (val: string) => !extensions.validateNoPort(val),
            message: (params: any, address: KnockoutObservable<string>) => {
                return extensions.validateNoPort(address());
            }
        };
        
        (ko.validation.rules as any)['validAddressWithoutPort'] = {
            validator: (val: string) => !extensions.validateAddressAndNoPort(val),
            message: (params: any, ipAddress: KnockoutObservable<string>) => {
                return extensions.validateAddressAndNoPort(ipAddress());
            }
        };

        (ko.validation.rules as any)['base64'] = {
            validator: (val: string) => {
                const base64regex = /^([0-9a-zA-Z+/]{4})*(([0-9a-zA-Z+/]{2}==)|([0-9a-zA-Z+/]{3}=))?$/;
                return !val || base64regex.test(val);
            },
            message: 'Invaild base64 string.'
        };
        
        (ko.validation.rules as any)['validLicense'] = {
            validator: (license: string) => {
                try {
                    const parsedLicense = JSON.parse(license);

                    const hasId = "Id" in parsedLicense;
                    const hasName = "Name" in parsedLicense;
                    const hasKeys = "Keys" in parsedLicense;

                    return hasId && hasName && hasKeys;
                } catch (e) {
                    return false;
                }
            },
            message: "Invalid license format"
        };

        (ko.validation.rules as any)['aceValidation'] = {
            validator: (text: string) => {
                // we return true here, as validation is handled in aceEditorBindingHandler
                return true; 
            }
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

        ko.bindingHandlers["tooltipText"] = {
            init: (element: any, valueAccessor: KnockoutObservable<string>) => {
                const text = ko.utils.unwrapObservable(valueAccessor());
                $(element).tooltip({
                    title: text,
                    container: element
                });
            },
            update: (element: any, valueAccessor: KnockoutObservable<string>) => {
                const text = ko.utils.unwrapObservable(valueAccessor());
                $(element).attr("data-original-title", text);
                $(".tooltip .tooltip-inner", element).html(text);
            }
        };

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
                
                if (valueUnwrapped) {
                    $element
                        .addClass('collapse')
                        .addClass('in')
                        .collapse({
                            toggle: !valueUnwrapped
                        });
                } else {
                    $element
                        .addClass('collapse')
                        .collapse({
                            toggle: valueUnwrapped
                        });
                }

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

                    const closestClosePanel = $target.closest(".close-panel");
                    const clickedOnClose = !!closestClosePanel.length;
                    if (clickedOnClose) {
                        if (!closestClosePanel.is(":disabled")) {
                            const $dropdownParent = $target.closest(".dropdown-menu").parent();
                            $dropdownParent.removeClass('open');
                        } else {
                            e.stopPropagation();
                        }
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
                    case "checked":
                        element.checked = true;
                        element.readOnly = false;
                        element.indeterminate = false;
                        break;
                    case "some_checked":
                        element.readOnly = true;
                        element.indeterminate = true;
                        element.checked = false;
                        break;
                    case "unchecked":
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
