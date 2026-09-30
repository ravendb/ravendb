import { useCallback } from "react";
import { useTranslation } from "react-i18next";
import { StudioTranslate, TranslationNamespace } from "common/i18n/resources";
import { TranslateOptions } from "common/i18n/i18n";

export function useStudioTranslation<Ns extends TranslationNamespace>(ns: Ns): StudioTranslate<Ns> {
    const { t } = useTranslation(ns);

    return useCallback(
        ((key: string, options?: TranslateOptions) =>
            t(key, { ...options, interpolation: { escapeValue: false } }) as string) as StudioTranslate<Ns>,
        [t]
    );
}
