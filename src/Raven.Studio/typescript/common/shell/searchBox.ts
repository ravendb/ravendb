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

    recentDocumentsList = ko.pureComputed(() => {
        const currentDb = activeDatabaseTracker.default.database();
        if (!currentDb) {
            return [];
        }

        return recentDocuments.getTopRecentDocumentsAsObservable(currentDb)();
    });
    
    matchedDocumentIds = ko.observableArray<string>([]);
    
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
        });

        this.searchQuery.throttle(250).subscribe(query => {
            this.matchedDocumentIds([]);
            
            if (query) {
                this.spinners.startsWith(true);
                new getDocumentsMetadataByIDPrefixCommand(query, 10, activeDatabaseTracker.default.database())
                    .execute()
                    .done((results: Array<metadataAwareDto>) => {
                        this.matchedDocumentIds(results.map(x => x['@metadata']['@id']));
                    })
                    .always(() => this.spinners.startsWith(false));
            }
        });
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
