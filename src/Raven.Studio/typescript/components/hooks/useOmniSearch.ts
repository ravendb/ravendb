import assertUnreachable from "components/utils/assertUnreachable";
import Fuse, { FuseResult, RangeTuple } from "fuse.js";
import { useCallback, useMemo, useState } from "react";

interface Action {
    text: string;
    alternativeTexts?: string[];
}

export interface OmniSearchItem<TType = unknown> extends Action {
    type: TType;
    innerActions?: Action[];
}

export interface OmniSearchResults<TItem> {
    items: OmniSearchResultItem<TItem>[];
}

export interface OmniSearchResultItem<TItem> {
    item: TItem;
    indices?: readonly RangeTuple[];
    innerActionText?: string;
    innerActionIndices?: readonly RangeTuple[];
}

type KeyName = keyof Action | `innerActions.${keyof Action}`;

interface UseOmniSearchArgs {
    resultsLimit?: number;
    threshold?: number;
}

export function useOmniSearch<TItem extends OmniSearchItem<TType>, TType>(args: UseOmniSearchArgs = {}) {
    const { resultsLimit = 15, threshold = 0.3 } = args;

    const [registeredItems, setRegisteredItems] = useState<TItem[]>([]);

    const engine = useMemo(() => {
        return new Fuse(registeredItems, {
            threshold,
            includeMatches: true,
            ignoreLocation: true,
            keys,
        });
    }, [registeredItems, threshold]);

    const register = useCallback((type: TType, newItems: TItem[]) => {
        setRegisteredItems((prev) => [...prev.filter((x) => x.type !== type), ...newItems]);
    }, []);

    const getInnerActionText = useCallback((result: FuseResult<TItem>, matchedValue: string) => {
        return result.item.innerActions?.find((x) => x.alternativeTexts?.includes(matchedValue))?.text;
    }, []);

    const search = useCallback(
        (input: string): OmniSearchResults<TItem> => {
            const rawResults = engine.search(input, { limit: resultsLimit });
            const items: OmniSearchResultItem<TItem>[] = [];

            const addItem = (item: TItem, additionalFields?: Partial<OmniSearchResultItem<TItem>>) => {
                items.push({ item, ...additionalFields });
            };

            for (const result of rawResults) {
                for (const match of result.matches) {
                    const key = match.key as KeyName;

                    switch (key) {
                        case "text": {
                            addItem(result.item, { indices: match.indices });
                            break;
                        }
                        case "alternativeTexts": {
                            if (
                                items.some((x) => x.item.type === result.item.type && x.item.text === result.item.text)
                            ) {
                                continue;
                            }
                            addItem(result.item);
                            break;
                        }
                        case "innerActions.text": {
                            addItem(result.item, { innerActionText: match.value, innerActionIndices: match.indices });
                            break;
                        }
                        case "innerActions.alternativeTexts": {
                            const innerActionText = getInnerActionText(result, match.value);
                            if (
                                items.some(
                                    (x) =>
                                        x.item.type === result.item.type &&
                                        x.item.text === result.item.text &&
                                        x.innerActionText === innerActionText
                                )
                            ) {
                                continue;
                            }

                            addItem(result.item, { innerActionText });
                            break;
                        }
                        default:
                            assertUnreachable(key);
                    }
                }
            }

            return { items };
        },
        [engine, resultsLimit, getInnerActionText]
    );

    return {
        register,
        search,
    };
}

const keys: Array<{ name: KeyName; weight: number }> = [
    {
        name: "text",
        weight: 0.7,
    },
    {
        name: "alternativeTexts",
        weight: 0.3,
    },
    {
        name: "innerActions.text",
        weight: 0.4,
    },
    {
        name: "innerActions.alternativeTexts",
        weight: 0.3,
    },
];
