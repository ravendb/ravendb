import enAboutView from "./locales/en/aboutView.json";
import enCommon from "./locales/en/common.json";
import enDocuments from "./locales/en/documents.json";
import enDocumentRefresh from "./locales/en/documentRefresh.json";
import enEditCustomSorter from "./locales/en/editCustomSorter.json";
import enFeatureAvailabilitySummary from "./locales/en/featureAvailabilitySummary.json";
import enRevisionsBin from "./locales/en/revisionsBin.json";
import enStudioGlobalConfiguration from "./locales/en/studioGlobalConfiguration.json";
import plAboutView from "./locales/pl/aboutView.json";
import plCommon from "./locales/pl/common.json";
import plDocuments from "./locales/pl/documents.json";
import plDocumentRefresh from "./locales/pl/documentRefresh.json";
import plEditCustomSorter from "./locales/pl/editCustomSorter.json";
import plFeatureAvailabilitySummary from "./locales/pl/featureAvailabilitySummary.json";
import plRevisionsBin from "./locales/pl/revisionsBin.json";
import plStudioGlobalConfiguration from "./locales/pl/studioGlobalConfiguration.json";

export const supportedLanguages = ["en", "pl"] as const;

export type StudioLanguage = (typeof supportedLanguages)[number];

export const languageNames: Record<StudioLanguage, string> = {
    en: "English",
    pl: "Polski",
};

export const defaultNS = "common";

const en = {
    aboutView: enAboutView,
    common: enCommon,
    documentRefresh: enDocumentRefresh,
    documents: enDocuments,
    editCustomSorter: enEditCustomSorter,
    featureAvailabilitySummary: enFeatureAvailabilitySummary,
    revisionsBin: enRevisionsBin,
    studioGlobalConfiguration: enStudioGlobalConfiguration,
};

const pl: typeof en = {
    aboutView: plAboutView,
    common: plCommon,
    documentRefresh: plDocumentRefresh,
    documents: plDocuments,
    editCustomSorter: plEditCustomSorter,
    featureAvailabilitySummary: plFeatureAvailabilitySummary,
    revisionsBin: plRevisionsBin,
    studioGlobalConfiguration: plStudioGlobalConfiguration,
};

export const resources = { en, pl } satisfies Record<StudioLanguage, typeof en>;

export const namespaces = Object.keys(en);

export type TranslationResources = typeof en;

export type TranslationNamespace = keyof TranslationResources;

type LeafKeys<T> = {
    [K in keyof T & string]: T[K] extends string ? K : `${K}.${LeafKeys<T[K]>}`;
}[keyof T & string];

type WithSuffixBase<K extends string> = K | (K extends `${infer Base}_${string}` ? Base : never);

export type NamespaceKey<Ns extends TranslationNamespace> = WithSuffixBase<LeafKeys<TranslationResources[Ns]>>;

export type TranslationKey = {
    [Ns in TranslationNamespace]: `${Ns}:${NamespaceKey<Ns>}`;
}[TranslationNamespace];
