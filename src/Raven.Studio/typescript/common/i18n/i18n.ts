import i18next, { i18n as I18nInstance, TOptions } from "i18next";
import {
    defaultNS,
    en,
    languageLoaders,
    namespaces,
    StudioLanguage,
    StudioTranslate,
    supportedLanguages,
    TranslationNamespace,
} from "./resources";

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
        resources: { en },
        initAsync: false,
        returnNull: false,
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

export async function loadLanguage(language: StudioLanguage): Promise<void> {
    if (language === "en" || i18n.hasResourceBundle(language, defaultNS)) {
        return;
    }

    const resources = await languageLoaders[language]();
    Object.entries(resources).forEach(([ns, bundle]) => i18n.addResourceBundle(language, ns, bundle));
}

export async function changeLanguage(language: StudioLanguage): Promise<void> {
    await loadLanguage(language);
    await i18n.changeLanguage(language);
}

/**
 * Translator bound to a namespace for non-React code (Knockout viewmodels, helpers, commands).
 * Interpolated values are HTML-escaped, so the result is safe for `html:` bindings and toasts.
 * React components use the `useStudioTranslation` hook instead.
 */
export function createTranslator<Ns extends TranslationNamespace>(ns: Ns): StudioTranslate<Ns> {
    const fixedT = i18n.getFixedT(null, ns);
    return ((key: string, options?: TranslateOptions) => fixedT(key, options) as string) as StudioTranslate<Ns>;
}
