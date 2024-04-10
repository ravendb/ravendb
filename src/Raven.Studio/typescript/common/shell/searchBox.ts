/*
    Events emitted through ko.postbox
        * SearchBox.Show - when searchbox is opened
        * SearchBox.Hide - when searchbox is hidden
*/

import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import { OmniSearch } from "common/omniSearch/omniSearch";
import leafMenuItem from "common/shell/menu/leafMenuItem";
import intermediateMenuItem from "common/shell/menu/intermediateMenuItem";
import collection from "models/database/documents/collection";
import accessManager from "common/shell/accessManager";
import { exhaustiveStringTuple } from "components/utils/common";
import assertUnreachable from "components/utils/assertUnreachable";

type SearchItemType = "databaseMenuItem" | "serverMenuItem" | "collection" | "index" | "recentDocument" | "document";

type SearchInnerAction = {
    text: string;
    alternativeTexts?: string[];
}

type SearchItem = {
    type: SearchItemType;
    icon: string;
    onSelected: (_: unknown, event: JQueryMouseEventObject) => void;
    text: string;
    alternativeTexts?: string[];
    innerActions?: SearchInnerAction[];
}

type SearchResultItem = {
    type: SearchItemType;
    icon: string;
    onSelected: (_: unknown, event: JQueryMouseEventObject) => void;
    text: string;
    subText?: string;
}

class searchBox {
    private readonly omniSearch = new OmniSearch<SearchItem, SearchItemType>();
    
    readonly allSearchTypes = exhaustiveStringTuple<SearchItemType>()("serverMenuItem", "databaseMenuItem", "collection", "index", "document", "recentDocument");

    searchQuery = ko.observable<string>();
    searchQueryHasFocus = ko.observable<boolean>(false);
    showSearchSmallScreen = ko.observable<boolean>(false);

    results = new Map<SearchItemType, KnockoutObservableArray<SearchResultItem>>();
    
    highlightedItem = ko.observable<{ index: number, listing: SearchItemType }>(null);
    
    spinners = {
        startsWith: ko.observable<boolean>(false)
    };
    
    dropdownVisible = ko.pureComputed(() => {
        const loading = this.spinners.startsWith();
        const anyItem = Array.from(this.results.values()).some(x => x().length > 0);
        return loading || anyItem;
    });

    private $searchContainer: JQuery;
    private $searchInput: JQuery;
    private readonly hideHandler = (e: Event) => {
        if (this.shouldConsumeHideEvent(e)) {
            this.hide()
        }
    };
    
    constructor() {
        _.bindAll(this, "goToDocument");
        
        this.allSearchTypes.forEach(type => this.results.set(type, ko.observableArray([])));
    }
    
    formatGroup(item: SearchItemType) {
        switch (item) {
            case "document":
                return "Documents";
            case "collection":
                return "Collections";
            case "index":
                return "Indexes";
            case "serverMenuItem":
                return "Server";
            case "databaseMenuItem":
                return "Current Database";
            case "recentDocument":
                return "Recent Documents";
            default:
                assertUnreachable(item);
        }
    }

    initialize() {
        this.$searchInput = $('.search-container input[type=search]');
        this.$searchContainer = $('.search-container');

        this.$searchInput.click((e) => {
            e.stopPropagation();
            this.show();
            
            this.highlightFirst();
        });
        
        this.$searchInput.keydown(e => {
            if (e.key === "ArrowDown" || e.key === "ArrowUp") {
                this.changeHighlightedItem(e.key === "ArrowDown" ? "down" : "up", [
                    {
                        listName: "matchedDocument",
                        list: this.matchedDocumentIds()
                    }, {
                        listName: "recentDocument",
                        list: this.recentDocumentsList()
                    }
                ]);
                return false; // prevent default
            } else if (e.key === "Enter") {
                this.searchQueryHasFocus(false);
                this.dispatchGoToItem(e.ctrlKey);
                return false;
            }
            
            return true;
        });

        this.searchQuery.subscribe(() => {
            this.highlightedItem(null);
        })
        
        this.searchQuery.throttle(250).subscribe(query => {
            if (query) {
                this.spinners.startsWith(true);
                const searchResults = this.omniSearch.search(query);
                const groups = _.uniq(searchResults.items.map(x => x.item.type));
                groups.forEach(group => {
                    const resultsByType: SearchResultItem[] = searchResults
                        .items
                        .filter(x => x.item.type === group)
                        .map(x => ({
                            ...x.item,
                            subText: x.matchedAlternative
                        }));
                    this.results.get(group)(resultsByType);
                });
                
                new getDocumentsMetadataByIDPrefixCommand(query, 10, activeDatabaseTracker.default.database())
                    .execute()
                    .done((results: Array<metadataAwareDto>) => {
                        const mappedResults = results.map(x => x['@metadata']['@id']);
                        const documents: SearchResultItem[] = mappedResults.map(result => ({
                            type: "document",
                            icon: "icon-document",
                            text: result,
                            onSelected: (_: unknown, event: JQueryMouseEventObject) => this.goToDocument(result, event),
                            subText: null
                        }));
                        this.results.get("document")(documents);

                        this.highlightFirst();
                    })
                    .always(() => this.spinners.startsWith(false));
            } else {
                Array.from(this.results.values()).forEach(v => v([]));
            }
        });
    }
    
    onMenuUpdated(items: menuItem[]) {
        const searchItems: SearchItem[] = [];
        const menuLeafs: leafMenuItem[] = [];

        const activeDatabase = activeDatabaseTracker.default.database();
        const activeDatabaseName = activeDatabase?.name ?? null;
        
        const crawlMenu = (item: menuItem) => {
            if (item instanceof leafMenuItem) {
                menuLeafs.push(item);
            } else if (item instanceof intermediateMenuItem) {
                item.children.forEach(crawlMenu);
            }
        }
        
        items.forEach(crawlMenu);

        menuLeafs.forEach(item => {
            if (ko.unwrap(item.nav) && !item.alias) {
                const canHandle = item.requiredAccess ? accessManager.canHandleOperation(item.requiredAccess, activeDatabaseName) : true;

                if (canHandle) {
                    const firstRoute = (Array.isArray(item.route) ? item.route[0] : item.route) ?? "";
                    const isDatabaseRoute = searchBox.isDatabaseRoute(firstRoute);

                    if (isDatabaseRoute && !activeDatabaseName) {
                        // skip this item
                        return ;
                    }
                    
                    searchItems.push({
                        type: isDatabaseRoute ? "databaseMenuItem" : "serverMenuItem",
                        text: item.title,
                        alternativeTexts: item.search?.alternativeTitles ?? [],
                        icon: item.css,
                        onSelected: (_: unknown, event: JQueryMouseEventObject) => this.goToMenuItem(item, event),
                        innerActions: (item.search?.innerActions ?? []).map(x => ({
                            text: x.name,
                            alternativeTexts: x.alternativeNames
                        }))
                    });
                }
            }
        });
        
        const itemsByType = _.groupBy(searchItems, x => x.type);
        Object.entries(itemsByType).forEach(([type, items]) =>  {
            this.omniSearch.register(type as SearchItemType, items);
        });
    }
    
    private static isDatabaseRoute(route: string): boolean {
        if (route === "databases") {
            return false;
        }
        return route.startsWith("databases");
    }
    
    onCollectionsUpdated(items: collection[]) {
        const searchItems: SearchItem[] = items.map(item => {
            return {
                text: item.name,
                onSelected: (_: unknown, event: JQueryMouseEventObject) => this.goToCollection(item.name, event),
                icon: "icon-documents",
                type: "collection"
            }
        });
        
        this.omniSearch.register("collection", searchItems);
    }
    onIndexesUpdated(indexNames: string[]) {
        const searchItems: SearchItem[] = indexNames.map(indexName => {
            return {
                text: indexName,
                onSelected: (_: unknown, event: JQueryMouseEventObject) => this.goToIndex(indexName, event),
                icon: "icon-index",
                type: "index"
            }
        });

        this.omniSearch.register("index", searchItems);
    private highlightFirst() {
        if (this.matchedDocumentIds().length) {
            this.highlightedItem({ index: 0, listing: "matchedDocument" });
        } else if (this.recentDocumentsList().length) {
            this.highlightedItem({ index: 0, listing: "recentDocument" });
        }
    }
    
    private changeHighlightedItem(direction: "up" | "down", items: Array<{ listName: "matchedDocument" | "recentDocument", list: Array<any>}>) {
        const highlight = this.highlightedItem();
        items = items.filter(x => x.list.length);
        
        if (!items.length) {
            // nothing to highlight
            this.highlightedItem(null);
            return;
        }
        
        if (!highlight) {
            switch (direction) {
                case "down": {
                    this.highlightedItem({
                        index: 0,
                        listing: _.first(items).listName
                    });
                    break;
                }
                case "up": {
                    const lastList = _.last(items);
                    this.highlightedItem({
                        index: lastList.list.length - 1,
                        listing: lastList.listName
                    });
                    break;
                }
            }
            
            return;
        }
        
        // at this point items contains not empty lists + we have highlighted item
        
        const currentListIdx = items.findIndex(x => x.listName === highlight.listing);
        if (direction === "down") {
            if (highlight.index < items[currentListIdx].list.length - 1) {
                this.highlightedItem({
                    index: highlight.index + 1,
                    listing: items[currentListIdx].listName
                })
            } else {
                // go to first item of next listing
                this.highlightedItem({
                    index: 0,
                    listing: items[(currentListIdx + 1) % items.length].listName
                });
            }
        } else { // up
            if (highlight.index > 0) {
                this.highlightedItem({
                    index: highlight.index - 1,
                    listing: highlight.listing
                })
            } else {
                // go to last item of previous listing
                const previousListingIdx = (items.length + currentListIdx - 1) % items.length; 
                this.highlightedItem({
                    index: items[previousListingIdx].list.length - 1,
                    listing: items[previousListingIdx].listName
                });
            }
    }
    
    private dispatchGoToItem(newTab: boolean) {
        const highlight = this.highlightedItem();
        if (highlight) {
            switch (highlight.listing) {
                case "recentDocument":
                    this.goToDocument(this.recentDocumentsList()[highlight.index], newTab);
                    break;
                case "matchedDocument":
                    this.goToDocument(this.matchedDocumentIds()[highlight.index], newTab);
                    break;
            }
        } else if (this.searchQuery()) {
            // user hit enter but values still loading
            // try to load document by name
            this.goToDocument(this.searchQuery(), newTab);
        }

    private goToMenuItem(item: leafMenuItem, event: JQueryMouseEventObject) {
        const url = item.dynamicHash();
        this.goToUrl(url, event.ctrlKey);
    }
    
    private goToCollection(collectionName: string, event: JQueryMouseEventObject) {
        const url = appUrl.forDocuments(collectionName, activeDatabaseTracker.default.database());
        this.goToUrl(url, event.ctrlKey);
    }
    
    private goToDocument(documentName: string, event: JQueryMouseEventObject) {
        const url = appUrl.forEditDoc(documentName, activeDatabaseTracker.default.database());
        this.goToUrl(url, event.ctrlKey);
    }

    private goToIndex(indexName: string, event: JQueryMouseEventObject) {
        const url = appUrl.forEditIndex(indexName, activeDatabaseTracker.default.database());
        return this.goToUrl(url, event.ctrlKey);
    }
    
    private goToUrl(url: string, newTab: boolean) {
        this.hide();
        this.searchQuery("");
        if (newTab) {
            window.open(url, "_blank").focus();
        } else {
            router.navigate(url);
        }
    }

    private show() {
        window.addEventListener("click", this.hideHandler, true);

        this.$searchContainer.addClass('active');
    }

    private hide() {
        window.removeEventListener("click", this.hideHandler, true);

        this.$searchContainer.removeClass('active');
    }

    private shouldConsumeHideEvent(e: Event) {
        return $(e.target).parents(".search-container").length === 0;
    }
}

export = searchBox;
