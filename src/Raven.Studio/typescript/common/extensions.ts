/// <reference path="../../typings/tsd.d.ts"/>
import virtualGrid = require("widgets/virtualGrid/virtualGrid");
import listView = require("widgets/listView/listView");
import genUtils = require("common/generalUtils");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import accessManager = require("common/shell/accessManager");

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
        if (!genUtils.urlRegex.test(url)) {
            return genUtils.invalidUrlMessage;
        }

        try {
            new URL(url);
        } catch (e) {
            return (e as Error).message;
        }

        return null;
    }
    
    private static validateAddress(address: string, allowedTypes: Array<addressType>, allowPort: boolean = true) : string {
        if (!address) {
            return null;
        }
        
        let typesText = "address";
        if (allowedTypes.length === 1 && allowedTypes[0] === "ipv4") {
            typesText = "IPv4 address";
        } else if (allowedTypes.length === 1 && allowedTypes[0] === "ipv6") {
            typesText = "IPv6 address";
        } else if (allowedTypes.length === 2 && _.includes(allowedTypes, "ipv4") && _.includes(allowedTypes, "ipv6")) {
            typesText = "IP address";
        }
        
        const addressInfo = genUtils.getAddressInfo(address);
        if (addressInfo.Type === "invalid" || !_.includes(allowedTypes, addressInfo.Type)) {
            return "Please enter a valid " + typesText;
        }
        
        if (addressInfo.HasPort && !allowPort) {
            return "Please enter an " + typesText + " without a port number";
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
        
        (ko.validation.rules as any)['numberOrNaN'] = {
            validator: (value: string, validate: any) => {
                if (!validate) { return true; }
                return ko.validation.utils.isEmptyVal(value) || (validate && /^-?(?:\d+|\d{1,3}(?:,\d{3})+)?(?:\.\d+)?$/.test(value)) || (validate && value === "NaN");
            },
            message: 'Please enter a number or NaN'
        };

        (ko.validation.rules as any)['validIpWithoutPort'] = {
            validator: (val: string) => !extensions.validateAddress(val, ["ipv4", "ipv6"], false),
            message: (params: any, address: KnockoutObservable<string>) => {
                return extensions.validateAddress(address(), ["ipv4", "ipv6"], false);
            }
        };
        
        (ko.validation.rules as any)['validAddressWithoutPort'] = {
            validator: (val: string) => !extensions.validateAddress(val, ["ipv4", "ipv6", "hostname"], false),
            message: (params: any, ipAddress: KnockoutObservable<string>) => {
                return extensions.validateAddress(ipAddress(), ["ipv4", "ipv6", "hostname"], false);
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
    
    private static verifyBindingLocation(bindingsArray: string[], binding: string, accessBindingLocation: number) {
        const bindingLocation = bindingsArray.indexOf(binding);

        if (bindingLocation > accessBindingLocation) {
            throw new Error(`The '${binding}' binding must come BEFORE the 'requiredAccess' binding in your html.`);
        }
    }
    
    private static installBindingHandlers() {
        ko.bindingHandlers["requiredAccess"] = {
            init: (element: Element,
                   valueAccessor: KnockoutObservable<Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess>,
                   allBindings) => {
                
                const requiredAccessLevel = ko.unwrap(valueAccessor());
                const databaseAccessLevelTypes = ["Read", "ReadWrite", "Admin", "Operator"];
                const securityClearanceLevelTypes = ["ValidUser", "Operator", "ClusterAdmin", "ClusterNode"];

                if (!_.includes(databaseAccessLevelTypes, requiredAccessLevel) && !_.includes(securityClearanceLevelTypes, requiredAccessLevel)) {
                    throw new Error(`Invalid Access Level. Value provided: ${requiredAccessLevel}.
                                     Possible Database Access values are: ${databaseAccessLevelTypes}.
                                     Possible Security Clearance values are: ${securityClearanceLevelTypes}.`);
                }
                
                const bindings = allBindings();
                const bindingsArray = Object.keys(bindings);
                const requiredAccessBindingLocation = bindingsArray.indexOf("requiredAccess");
                
                const activeDatabase = activeDatabaseTracker.default.database();
                if (activeDatabase) {
                    if (bindings.visible) {
                        this.verifyBindingLocation(bindingsArray, "visible", requiredAccessBindingLocation);
                    }
                    if (bindings.hidden) {
                        this.verifyBindingLocation(bindingsArray, "hidden", requiredAccessBindingLocation);
                    }
                    if (bindings.disable) {
                        this.verifyBindingLocation(bindingsArray, "disable", requiredAccessBindingLocation);
                    }
                    if (bindings.enable) {
                        this.verifyBindingLocation(bindingsArray, "enable", requiredAccessBindingLocation);
                    }
                } else {
                    throw new Error("Cannot use the 'requiredAccess' binding - no database is active.");
                }

                const strategyTypes = ["hide", "disable"]; // todo: "visibilityHidden"
                
                if (bindings.requiredAccessOptions) {
                    const strategy = bindings.requiredAccessOptions.strategy;
                    if (!_.includes(strategyTypes, strategy)) {
                        throw new Error(`Invalid requiredAccess strategy. Value provided: ${strategy}. Possible values are: ${strategyTypes}`);
                    }

                    if (strategy === 'disable') {
                        const hasDisabledClass = $(element).hasClass("disabled");
                        if (hasDisabledClass) {
                            throw new Error("Error in 'requiredAccess' binding. Support for 'disabled' class is not implemented.");
                        }

                        if (element.tagName === "A") {
                            throw new Error("Error in 'RequiredAccess' binding. Support for 'disable' strategy on type 'a' is not implemented.");
                        }
                        
                        if (bindings.enable && bindings.disable) {
                            throw new Error("Error in 'RequiredAccess' binding. Do not use both 'disable' & 'enable' bindings together.");
                        }
                    }
                }
            },
            update: (element: any,
                     valueAccessor: KnockoutObservable<Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess>,
                     allBindings) => {

                const activeDatabase = activeDatabaseTracker.default.database();
                if (activeDatabase) {
                    const bindings = allBindings();
                    
                    const requiredAccessLevel = ko.unwrap(valueAccessor());
                    const strategy = bindings.requiredAccessOptions ? bindings.requiredAccessOptions.strategy : 'hide';
                    
                    const securityClearanceLevelTypes = ["ValidUser", "Operator", "ClusterAdmin", "ClusterNode"];
                    const isSecurityClearance = _.includes(securityClearanceLevelTypes, requiredAccessLevel);
                    
                    switch (strategy) {
                        case 'hide': {
                            const visibleBinding = bindings.visible;
                            const visibleValue = visibleBinding != null ? ko.unwrap(visibleBinding) : true;

                            const hiddenBinding = bindings.hidden;
                            const hiddenValue = hiddenBinding != null ? ko.unwrap(hiddenBinding) : false;

                            const shouldBeVisibleByKo = visibleValue && !hiddenValue;
                            const isElementVisible = element.style.display !== "none";

                            if (accessManager.default.canHandleOperation(requiredAccessLevel, isSecurityClearance)()) {
                                if (!isElementVisible && shouldBeVisibleByKo) {
                                    element.style.display = "";
                                }
                            } else {
                                if (isElementVisible) {
                                    element.style.display = "none";
                                }
                            }
                        }
                            break;

                        case 'disable': {
                            const disableBinding = bindings.disable;
                            const disableValue = disableBinding != null ? ko.unwrap(disableBinding) : false;

                            const enableBinding = bindings.enable;
                            const enableValue = enableBinding != null ? ko.unwrap(enableBinding) : true;

                            const shouldBeEnabledByKo = !disableValue && enableValue;
                            const isElementDisabled = element.hasAttribute("disabled");

                            if (accessManager.default.canHandleOperation(requiredAccessLevel, isSecurityClearance)()) {
                                if (isElementDisabled && shouldBeEnabledByKo) {
                                    element.setAttribute("disabled", false)
                                }
                            } else {
                                if (!isElementDisabled) {
                                    element.setAttribute("disabled", true);
                                }
                            }
                        }
                            break;
                    }
                }
            }
        };
        
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
