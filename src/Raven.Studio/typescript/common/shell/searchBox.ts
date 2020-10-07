/*
    Events emitted through ko.postbox
        * SearchBox.Show - when searchbox is opened
        * SearchBox.Hide - when searchbox is hidden
*/

import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import recentDocuments = require("models/database/documents/recentDocuments");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import appUrl = require("common/appUrl");
import router = require("plugins/router");

class searchBox {

    searchQuery = ko.observable<string>();
    showSearchSmallScreen = ko.observable<boolean>(false);

    recentDocumentsList = ko.pureComputed(() => {
        const currentDb = activeDatabaseTracker.default.database();
        if (!currentDb) {
            return [];
        }

        return recentDocuments.getTopRecentDocumentsAsObservable(currentDb)();
    });
    
    matchedDocumentIds = ko.observableArray<string>([]);
    highlightedItem = ko.observable<{index: number, listing: "recentDocument" | "matchedDocument" }>(null);
    
    spinners = {
        startsWith: ko.observable<boolean>(false)
    };
    
    showMatchedDocumentsSection = ko.pureComputed(() => {
        const hasDocuments = this.matchedDocumentIds().length;
        const inProgress = this.spinners.startsWith();
        
        return hasDocuments || inProgress;
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
                this.dispatchGoToItem();
                return false;
            }
            
            return true;
        });

        this.searchQuery.throttle(250).subscribe(query => {
            this.highlightedItem(null);
            this.matchedDocumentIds([]);
            
            if (query) {
                this.spinners.startsWith(true);
                new getDocumentsMetadataByIDPrefixCommand(query, 10, activeDatabaseTracker.default.database())
                    .execute()
                    .done((results: Array<metadataAwareDto>) => {
                        const mappedResults = results.map(x => x['@metadata']['@id']);
                        this.matchedDocumentIds(mappedResults);

                        this.highlightFirst();
                    })
                    .always(() => this.spinners.startsWith(false));
            }
        });
    }
    
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
                case "down":
                    this.highlightedItem({
                        index: 0,
                        listing: _.first(items).listName
                    });
                    break;
                case "up":
                    const lastList = _.last(items);
                    this.highlightedItem({
                        index: lastList.list.length - 1,
                        listing: lastList.listName
                    });
                    break;
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
    }

    matchesDocumentIdx(idx: KnockoutObservable<number>) {
        return ko.pureComputed(() => {
            const highlight = this.highlightedItem();
            if (highlight && highlight.listing === "matchedDocument") {
                return ko.unwrap(idx) === highlight.index;
            }
            
            return false;
        })
    }

    matchesRecentDocumentIdx(idx: KnockoutObservable<number>) {
        return ko.pureComputed(() => {
            const highlight = this.highlightedItem();
            if (highlight && highlight.listing === "recentDocument") {
                return ko.unwrap(idx) === highlight.index;
            }
            
            return false;
        })
    }
    
    private dispatchGoToItem() {
        const highlight = this.highlightedItem();
        if (highlight) {
            switch (highlight.listing) {
                case "recentDocument":
                    this.goToDocument(this.recentDocumentsList()[highlight.index]);
                    break;
                case "matchedDocument":
                    this.goToDocument(this.matchedDocumentIds()[highlight.index]);
                    break;
            }
        }
    }
    
    goToDocument(documentName: string) {
        const url = appUrl.forEditDoc(documentName, activeDatabaseTracker.default.database());
        this.hide();
        this.searchQuery("");
        router.navigate(url);
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
