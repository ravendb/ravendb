import i18next, { i18n as I18nInstance, TOptions } from "i18next";
import { defaultNS, namespaces, resources, supportedLanguages, TranslationKey } from "./resources";

export type MissingKeyBehavior = "warn" | "throw";

export interface InitI18nOptions {
    onMissingKey?: MissingKeyBehavior;
}

export type TranslateOptions = TOptions;

export const i18n: I18nInstance = i18next.createInstance();

export function initI18n(options?: InitI18nOptions): I18nInstance {
    if (i18n.isInitialized) {
        return i18n;
    }

    const onMissingKey: MissingKeyBehavior = options?.onMissingKey ?? "warn";

    i18n.init({
        lng: "en",
        fallbackLng: "en",
        supportedLngs: supportedLanguages,
        ns: namespaces,
        defaultNS,
        resources,
        initAsync: false,
        returnNull: false,
        interpolation: {
            escapeValue: false,
        },
        parseMissingKeyHandler: (key: string) => {
            const message = `[i18n] Missing translation key: ${key}`;
            if (onMissingKey === "throw") {
                throw new Error(message);
            }
            console.warn(message);
            return key;
        },
    });

    return i18n;
}

/**
 * Typed translation for non-React code (Knockout viewmodels, helpers, commands).
 * React components use the `useStudioTranslation` hook instead.
 */
export function translate(key: TranslationKey, options?: TranslateOptions): string {
    return i18n.t(key, options) as string;
}
