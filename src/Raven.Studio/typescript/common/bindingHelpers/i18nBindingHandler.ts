/// <reference path="../../../typings/tsd.d.ts" />

import i18nModule = require("common/i18n/i18n");

type i18nKeyOptions = Record<string, unknown>;

type i18nBindingValue =
    | null
    | undefined
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
 * A null or undefined value clears the text or removes the attribute.
 */
class i18nBindingHandler {
    private static installed = false;

    static install() {
        if (i18nBindingHandler.installed) {
            return;
        }
        i18nBindingHandler.installed = true;

        ko.bindingHandlers["i18n"] = {
            update(element: HTMLElement, valueAccessor: () => i18nBindingValue) {
                element.textContent = i18nBindingHandler.resolve(valueAccessor()) ?? "";
            },
        };

        ko.bindingHandlers["i18nAttr"] = {
            update(element: HTMLElement, valueAccessor: () => i18nAttrBindingValue) {
                const attributes = ko.unwrap(valueAccessor());
                Object.entries(attributes).forEach(([attribute, value]) => {
                    const text = i18nBindingHandler.resolve(value);
                    if (text == null) {
                        element.removeAttribute(attribute);
                    } else {
                        element.setAttribute(attribute, text);
                    }
                });
            },
        };
    }

    static resolve(value: i18nBindingValue): string | null {
        i18nModule.currentLanguage();

        const unwrapped = ko.unwrap(value);

        if (unwrapped == null) {
            return null;
        }

        if (typeof unwrapped === "string") {
            return i18nBindingHandler.translate(unwrapped);
        }

        const key = ko.unwrap(unwrapped.key);
        const rawOptions = ko.unwrap(unwrapped.options);
        const options = rawOptions ? _.mapValues(rawOptions, (option: unknown) => ko.unwrap(option)) : undefined;

        return i18nBindingHandler.translate(key, options);
    }

    private static translate(key: string, options?: i18nKeyOptions): string {
        if (!key.includes(":")) {
            throw new Error(`[i18n] Knockout translation key must have a namespace: ${key}`);
        }
        return i18nModule.i18n.t(key, { ...options, interpolation: { escapeValue: false } }) as string;
    }
}

export = i18nBindingHandler;
