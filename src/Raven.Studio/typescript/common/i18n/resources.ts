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

export function isStudioLanguage(value: unknown): value is StudioLanguage {
    return supportedLanguages.includes(value as StudioLanguage);
}

export const languageNames: Record<StudioLanguage, string> = {
    en: "English",
    pl: "Polski",
};

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

type PartialResources<T> = { [K in keyof T]?: T[K] extends string ? string : PartialResources<T[K]> };

export type LanguageResources = PartialResources<TranslationResources>;

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

type KeyParams<Ns extends TranslationNamespace, Key> = Key extends unknown
    ? VariantParams<Extract<Variant<Ns>, { base: Key }>>
    : never;

type BaseOf<V> = V extends { base: infer Base extends string } ? Base : never;

export type TranslationKey<Ns extends TranslationNamespace> = BaseOf<Variant<Ns>>;

type UnionToIntersection<U> = (U extends unknown ? (union: U) => void : never) extends (intersection: infer I) => void
    ? I
    : never;

type TranslateParams<Ns extends TranslationNamespace, Key> = UnionToIntersection<KeyParams<Ns, Key>>;

type TranslateArgs<Params> = [keyof Params] extends [never]
    ? []
    : object extends Params
      ? [options?: Params]
      : [options: Params];

export type StudioTranslate<Ns extends TranslationNamespace> = <Key extends TranslationKey<Ns>>(
    key: Key,
    ...args: TranslateArgs<TranslateParams<Ns, Key>>
) => string;

type OptionsProp<Args> = Args extends []
    ? { options?: never }
    : Args extends [options: infer Params]
      ? { options: Params }
      : Args extends [options?: infer Params]
        ? { options?: Params }
        : never;

export type TranslateOptionsProp<Ns extends TranslationNamespace, Key> = OptionsProp<
    TranslateArgs<TranslateParams<Ns, Key>>
>;
