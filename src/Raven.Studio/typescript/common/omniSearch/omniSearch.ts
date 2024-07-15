import assertUnreachable from "components/utils/assertUnreachable";
import Fuse, { FuseResult, RangeTuple } from "fuse.js";

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

export class OmniSearch<TItem extends OmniSearchItem<TType>, TType> {
    private readonly engine: Fuse<TItem>;

    public constructor() {
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

        this.engine = new Fuse<TItem>([], {
            includeMatches: true,
            threshold: 0.3,
            ignoreLocation: true,
            keys,
        });
    }

    public register(type: TType, items: TItem[]) {
        this.engine.remove((doc) => doc.type === type);
        items.forEach((x) => this.engine.add(x));
    }

    public search(input: string): OmniSearchResults<TItem> {
        const rawResults = this.engine.search(input, { limit: 15 });
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
                        if (items.some((x) => x.item.type === result.item.type && x.item.text === result.item.text)) {
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
                        const innerActionText = this.getInnerActionText(result, match.value);
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
    }

    private getInnerActionText = (result: FuseResult<TItem>, matchedValue: string) => {
        return result.item.innerActions?.find((x) => x.alternativeTexts?.includes(matchedValue))?.text;
    };
}
