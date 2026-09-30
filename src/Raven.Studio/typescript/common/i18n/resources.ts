import aboutView from "./locales/en/aboutView";
import common from "./locales/en/common";
import conflicts from "./locales/en/conflicts";
import documentRefresh from "./locales/en/documentRefresh";
import editCustomSorter from "./locales/en/editCustomSorter";
import featureAvailabilitySummary from "./locales/en/featureAvailabilitySummary";
import revisionsBin from "./locales/en/revisionsBin";
import studioGlobalConfiguration from "./locales/en/studioGlobalConfiguration";

export const supportedLanguages = ["en", "pl"] as const;

export type StudioLanguage = (typeof supportedLanguages)[number];

export const languageNames: Record<StudioLanguage, string> = {
    en: "English",
    pl: "Polski",
};

export const defaultNS = "common";

export const en = {
    aboutView,
    common,
    conflicts,
    documentRefresh,
    editCustomSorter,
    featureAvailabilitySummary,
    revisionsBin,
    studioGlobalConfiguration,
};

export type TranslationResources = typeof en;

export type TranslationNamespace = keyof TranslationResources;

type Widen<T> = { [K in keyof T]: T[K] extends string ? string : Widen<T[K]> };

export type LanguageResources = Widen<TranslationResources>;

export const languageLoaders: Record<Exclude<StudioLanguage, "en">, () => Promise<LanguageResources>> = {
    pl: async () => (await import("./locales/pl")).default,
};

export const namespaces = Object.keys(en);

type PluralSuffix = "zero" | "one" | "two" | "few" | "many" | "other";

type Entries<T, Prefix extends string = ""> = {
    [K in keyof T & string]: T[K] extends string
        ? { key: `${Prefix}${K}`; value: T[K] }
        : Entries<T[K], `${Prefix}${K}.`>;
}[keyof T & string];

type Variables<Value> = Value extends `${string}{{${infer Name}}}${infer Rest}` ? Name | Variables<Rest> : never;

type WithContext<Key extends string, Vars> = Key extends `${infer Base}_${infer Context}`
    ? { base: Base; context: Context; vars: Vars }
    : { base: Key; context: never; vars: Vars };

type KeyVariant<Entry> = Entry extends { key: infer Key extends string; value: infer Value }
    ? Key extends `${infer Base}_${PluralSuffix}`
        ? WithContext<Base, Variables<Value> | "count">
        : WithContext<Key, Variables<Value>>
    : never;

type Variant<Ns extends TranslationNamespace> = KeyVariant<Entries<TranslationResources[Ns]>>;

type ContextParam<V extends { context: string }> = [V["context"]] extends [never]
    ? unknown
    : [Extract<V, { context: never }>] extends [never]
      ? { context: V["context"] }
      : { context?: V["context"] };

type VariantParams<V extends { context: string; vars: string }> = {
    [Name in V["vars"]]: Name extends "count" ? number : string | number;
} & ContextParam<V>;

type KeyParams<Ns extends TranslationNamespace, Key> = VariantParams<Extract<Variant<Ns>, { base: Key }>>;

type BaseOf<V> = V extends { base: infer Base extends string } ? Base : never;

export type NamespaceKey<Ns extends TranslationNamespace> = BaseOf<Variant<Ns>>;

type CommonKey = `${typeof defaultNS}:${NamespaceKey<typeof defaultNS>}`;

type ScopedKeyParams<Ns extends TranslationNamespace, Key> = Key extends `${typeof defaultNS}:${infer CommonKeyName}`
    ? KeyParams<typeof defaultNS, CommonKeyName>
    : KeyParams<Ns, Key>;

type TranslateArgs<Params> = [keyof Params] extends [never]
    ? []
    : object extends Params
      ? [options?: Params]
      : [options: Params];

export type StudioTranslate<Ns extends TranslationNamespace> = <Key extends NamespaceKey<Ns> | CommonKey>(
    key: Key,
    ...args: TranslateArgs<ScopedKeyParams<Ns, Key>>
) => string;
