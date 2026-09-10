import { useCallback } from "react";
import { useTranslation } from "react-i18next";
import { NamespaceKey, TranslationNamespace } from "common/i18n/resources";
import { TranslateOptions } from "common/i18n/i18n";

export type StudioTranslate<Ns extends TranslationNamespace> = (
    key: NamespaceKey<Ns>,
    options?: TranslateOptions
) => string;

export function useStudioTranslation<Ns extends TranslationNamespace>(ns: Ns) {
    const { t, i18n } = useTranslation(ns);

    const translate = useCallback<StudioTranslate<Ns>>((key, options) => t(key as string, options) as string, [t]);

    return { t: translate, i18n };
}
