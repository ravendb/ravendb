import Fuse from "fuse.js";

export interface OmniSearchItem<TType> {
    text: string;
    alternativeTexts?: string[];
    innerActions?: Array<{
        text: string;
        alternativeTexts?: string[];
    }>
    type: TType;
}

export interface OmniSearchResults<TItem> {
    items: OmniSearchResultItem<TItem>[];
}

export interface OmniSearchResultItem<TItem> {
    item: TItem;
    matchedAlternative?: string;
}

type KeyName = keyof OmniSearchItem<unknown> | `innerActions.${keyof OmniSearchItem<unknown>["innerActions"][0]}`;

export class OmniSearch<TItem extends OmniSearchItem<TType>, TType> {
    private readonly engine: Fuse<TItem>;
    
    public constructor() {
        const keys: Array<{ name: KeyName, weight: number }> = [
            {
                name: "text",
                weight: 0.7
            },
            {
                name: "alternativeTexts",
                weight: 0.3
            },
            {
                name: "innerActions.text",
                weight: 0.4,
            },
            {
                name: "innerActions.alternativeTexts",
                weight: 0.3
            }
        ]
        
        this.engine = new Fuse<TItem>([], {
            includeMatches: true,
            threshold: 0.2,
            ignoreLocation: true,
            shouldSort: true,
            isCaseSensitive: false,
            keys
        });
    }
    
    public register(type: TType, items: TItem[]) {
        this.engine.remove(doc => doc.type === type);
        items.forEach(x => this.engine.add(x));
    }
    
    public search(input: string): OmniSearchResults<TItem> {
        const rawResults = this.engine.search(input, {
            limit: 15
        });
        
        const items: OmniSearchResultItem<TItem>[] = [];
        
        rawResults.forEach(result => {
            const hasGlobalMatch = result.matches.some(x => x.key === "text");
            
            result.matches.forEach(match => {
                const matchKey = match.key as KeyName;
                if (matchKey === "text") {
                    items.push({
                        item: result.item,
                        matchedAlternative: undefined
                    });
                }

                if (hasGlobalMatch) {
                    // show inner items only when there isn't global match to avoid duplicates
                    return;
                }
                
                if (matchKey === "alternativeTexts") {
                    items.push({
                        item: result.item,
                        matchedAlternative: undefined
                    });
                } else if (matchKey === "innerActions.text") {
                    items.push({
                        item: result.item,
                        matchedAlternative: match.value
                    });
                } else if (matchKey === "innerActions.alternativeTexts") {
                    const mappedAlternatives = result.item.innerActions.flatMap(x => x.alternativeTexts.map(t => [x.text, t] as const));
                    const matchItem = mappedAlternatives[match.refIndex];

                    items.push({
                        item: result.item,
                        matchedAlternative: matchItem[0]
                    })
                } else {
                    throw new Error("Unknown match: " + match);
                }
            });
        });
        
        return {
            items: items
        }
    }
}
