import React from "react";
import { Trans } from "react-i18next";
import { NamespaceKey, TranslationNamespace } from "common/i18n/resources";

interface StudioTransProps<Ns extends TranslationNamespace> {
    ns: Ns;
    i18nKey: NamespaceKey<Ns>;
    components?: Record<string, React.ReactElement>;
    values?: Record<string, unknown>;
}

export function StudioTrans<Ns extends TranslationNamespace>({
    ns,
    i18nKey,
    components,
    values,
}: StudioTransProps<Ns>) {
    return <Trans ns={ns} i18nKey={i18nKey} components={components} values={values} />;
}
