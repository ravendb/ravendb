/// <reference path="../../../typings/tsd.d.ts" />

import i18nModule = require("common/i18n/i18n");

type i18nKeyOptions = Record<string, unknown>;

type i18nBindingValue =
    | string
    | KnockoutObservable<string>
    | {
          key: string | KnockoutObservable<string>;
          options?: i18nKeyOptions | KnockoutObservable<i18nKeyOptions>;
      };

type i18nAttrBindingValue = Record<string, i18nBindingValue>;

/**
 * Knockout bindings for translations.
 *
 *   <span data-bind="i18n: 'common:save'"></span>
 *   <h3 data-bind="i18n: { key: 'editCustomSorter:heading', options: { context: isNew() ? 'new' : 'edit' } }"></h3>
 *   <input data-bind="i18nAttr: { placeholder: 'editCustomSorter:namePlaceholder', title: 'common:cancel' }">
 *
 * Keys use the "namespace:key" form. `key`, `options` and option values may be observables.
 * Text is written to textContent, never innerHTML. Bindings re-evaluate on language change.
 */
class i18nBindingHandler {
    private static installed = false;

    private static readonly languageVersion = ko.observable(0);

    static install() {
        if (i18nBindingHandler.installed) {
            return;
        }
        i18nBindingHandler.installed = true;

        i18nModule.i18n.on("languageChanged", () => {
            i18nBindingHandler.languageVersion(i18nBindingHandler.languageVersion() + 1);
        });

        ko.bindingHandlers["i18n"] = {
            update(element: HTMLElement, valueAccessor: () => i18nBindingValue) {
                element.textContent = i18nBindingHandler.resolve(valueAccessor());
            },
        };

        ko.bindingHandlers["i18nAttr"] = {
            update(element: HTMLElement, valueAccessor: () => i18nAttrBindingValue) {
                const attributes = ko.unwrap(valueAccessor());
                Object.entries(attributes).forEach(([attribute, value]) => {
                    element.setAttribute(attribute, i18nBindingHandler.resolve(value));
                });
            },
        };
    }

    static resolve(value: i18nBindingValue): string {
        i18nBindingHandler.languageVersion();

        const unwrapped = ko.unwrap(value);

        if (typeof unwrapped === "string") {
            return i18nModule.i18n.t(unwrapped) as string;
        }

        const key = ko.unwrap(unwrapped.key);
        const rawOptions = ko.unwrap(unwrapped.options);
        const options = rawOptions ? _.mapValues(rawOptions, (option: unknown) => ko.unwrap(option)) : undefined;

        return i18nModule.i18n.t(key, options as i18nModule.TranslateOptions) as string;
    }
}

export = i18nBindingHandler;
