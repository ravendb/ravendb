import i18next, { i18n as I18nInstance, TOptions } from "i18next";
import { initReactI18next } from "react-i18next";
import {
    en,
    isStudioLanguage,
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

    i18n.use(initReactI18next).init({
        lng: "en",
        fallbackLng: "en",
        supportedLngs: supportedLanguages,
        ns: namespaces,
        resources: { en },
        initAsync: false,
        returnNull: false,
        interpolation: { escapeValue: false },
        appendNamespaceToMissingKey: true,
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
    if (language === "en" || namespaces.some((ns) => i18n.hasResourceBundle(language, ns))) {
        return;
    }

    const resources = await languageLoaders[language]();
    Object.entries(resources).forEach(([ns, bundle]) => i18n.addResourceBundle(language, ns, bundle));
}

export async function changeLanguage(language: StudioLanguage): Promise<void> {
    const supportedLanguage = isStudioLanguage(language) ? language : "en";
    await loadLanguage(supportedLanguage);
    await i18n.changeLanguage(supportedLanguage);
}

/**
 * Translator bound to a namespace for non-React code (Knockout viewmodels, helpers, commands).
 * Returns plain text: interpolated values are not HTML-escaped, so escape before passing the result to an `html:` binding.
 * The language is set once before the shell loads and changing it reloads Studio,
 * so call `t()` lazily (in constructors, methods, computeds), never at module scope.
 * React components use the `useStudioTranslation` hook instead.
 */
export function createTranslator<Ns extends TranslationNamespace>(ns: Ns): StudioTranslate<Ns> {
    const fixedT = i18n.getFixedT(null, ns);
    return ((key: string, options?: TranslateOptions) => fixedT(key, options) as string) as StudioTranslate<Ns>;
}
