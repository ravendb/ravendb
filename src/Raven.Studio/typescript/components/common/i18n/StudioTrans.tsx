import React from "react";
import { Trans, useTranslation } from "react-i18next";
import { TranslateOptionsProp, TranslationKey, TranslationNamespace } from "common/i18n/resources";
import { TranslateOptions } from "common/i18n/i18n";

type StudioTransProps<Ns extends TranslationNamespace> = {
    [Key in TranslationKey<Ns>]: {
        ns: Ns;
        i18nKey: Key;
        components?: Record<string, React.ReactElement>;
    } & TranslateOptionsProp<Ns, Key>;
}[TranslationKey<Ns>];

/**
 * Renders a translation containing tags, mapping each tag to an element: `<StudioTrans ns="documentRefresh" i18nKey="about.intro" components={{ strong: <strong /> }} />`.
 * For plain text use `useStudioTranslation`; `i18nKey` and `options` are type-checked against `ns`.
 */
export function StudioTrans<Ns extends TranslationNamespace>({
    ns,
    i18nKey,
    components,
    options,
}: StudioTransProps<Ns>) {
    const { t } = useTranslation(ns);
    const { context, count, ...values } = (options ?? {}) as TranslateOptions;

    return (
        <Trans
            t={t}
            ns={ns}
            i18nKey={i18nKey}
            components={components}
            context={context as string}
            count={count}
            values={values}
        />
    );
}
