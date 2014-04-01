import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import pagedList = require("common/pagedList");
import filesystem = require("models/filesystem");
import collection = require("models/filesystemConfigurationKeyCollection");
import viewModelBase = require("viewmodels/viewModelBase");
import getFilesystemConfigurationCommand = require("commands/getFilesystemConfigurationCommand");

class filesystemConfiguration extends viewModelBase {

    private router = router;

    public static allItemsCollectionName = "All";
    private static styleMap: any = {};

    displayName = "configuration keys";
    keys = ko.observableArray<collection>();
    selectedKeys = ko.observable<collection>().subscribeTo("ActivateCollection").distinctUntilChanged();
    allKeysCollection: collection;
    collectionToSelectName: string;
    currentKeysPagedItems = ko.observable<pagedList>();
    selectedKeysIndices = ko.observableArray<number>();
    
    keyToSelectName: string;
    hasAnyKeysSelected: KnockoutComputed<boolean>;

    configurationUrl = appUrl.forCurrentDatabase().filesystemConfiguration;

    constructor() {
        super();

        this.selectedKeys.subscribe(c => this.selectedKeyChanged(c));
        this.hasAnyKeysSelected = ko.computed(() => this.selectedKeysIndices().length > 0);
    }

    activate(args) {
        super.activate(args);

        // We can optionally pass in a key name to view's URL, e.g. #/filesystems/configuration?key=Foo&filesystem="blahDb"
        this.keyToSelectName = args ? args.key : null;
        this.fetchKeys(appUrl.getFilesystem());
    }

    attached(view: HTMLElement, parent: HTMLElement) {
        // Initialize the context menu (using Bootstrap-ContextMenu library).
        // TypeScript doesn't know about Bootstrap-Context menu, so we cast jQuery as any.
        (<any>$('.keys-collections')).contextmenu({
            target: '#keys-context-menu'
        });
    }

    fetchKeys(fs: filesystem): JQueryPromise<Array<string>> {

        return new getFilesystemConfigurationCommand(fs)
                        .execute()
                        .done(results => this.keysLoaded(results, fs));
    }

    selectedKeyChanged(selected: collection) {

        if (selected) {
            var pagedList = selected.getItems();
            this.currentKeysPagedItems(pagedList);
        }
    }

    selectCollection(collection: collection) {

        collection.activate();
        var itemsWithCollectionUrl = appUrl.forFilesystemConfigurationWithKey(this.activeFilesystem(), collection.name);
        router.navigate(itemsWithCollectionUrl, false);
    }

    keysLoaded(collections: Array<collection>, fs: filesystem) {

        // Create the "All Keys" pseudo collection.
        this.allKeysCollection = new collection(filesystemConfiguration.allItemsCollectionName, fs);
        this.allKeysCollection.itemsCount = ko.computed(() =>
            this.keys()
                .filter(c => c !== this.allKeysCollection) // Don't include self, the all documents collection.
                .map(c => c.itemsCount()) // Grab the document count of each.
                .reduce((first: number, second: number) => first + second, 0)); // And sum them up.

        var allCollections = [this.allKeysCollection].concat(collections);
        this.keys(allCollections);

        // All systems a-go. Load them into the UI and select the first one.
        this.keys(collections);

        var collectionToSelect = allCollections.first(c => c.name === this.collectionToSelectName) || this.allKeysCollection;
        collectionToSelect.activate();
    }

    private static getCollectionCssClass(entityName: string): string {

        if (entityName === filesystemConfiguration.allItemsCollectionName) {
            return "all-items-collection";
        }

        var existingStyle = filesystemConfiguration.styleMap[entityName];
        if (existingStyle) {
            return existingStyle;
        }

        // We don't have an existing style. Assign one in the form of 'collection-style-X', where X is a number between 0 and maxStyleCount. These styles are found in app.less.
        var maxStyleCount = 16;
        var styleNumber = Object.keys(filesystemConfiguration.styleMap).length % maxStyleCount;
        var style = "collection-style-" + styleNumber;
        filesystemConfiguration.styleMap[entityName] = style;
        return style;
    }

} 

export = filesystemConfiguration;