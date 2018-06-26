import viewModelBase = require("viewmodels/viewModelBase");
import adminLogsWebSocketClient = require("common/adminLogsWebSocketClient");
import adminLogsConfig = require("models/database/debug/adminLogsConfig");
import adminLogsConfigEntry = require("models/database/debug/adminLogsConfigEntry");
import eventsCollector = require("common/eventsCollector");
import listViewController = require("widgets/listView/listViewController");
import fileDownloader = require("common/fileDownloader");
import virtualListRow = require("widgets/listView/virtualListRow");

class heightCalculator {
    
    private charactersPerLine = undefined as number;
    private padding = undefined as number;
    private lineHeight = undefined as number;
    
    measure(item: string, row: virtualListRow<string>) {
        this.ensureCacheFilled(row);
        
        const lines = item.split(/\r?\n/);
        const totalLinesCount = _.sum(lines.map(l => {
            if (l.length > this.charactersPerLine) {
                return Math.ceil(l.length  * 1.0 / this.charactersPerLine);
            }
            return 1;
        }));
        
        return this.padding + totalLinesCount * this.lineHeight;
    }
    
    // try to compute max numbers of characters in single line
    ensureCacheFilled(row: virtualListRow<string>) {
        if (!_.isUndefined(this.charactersPerLine)) {
            return;
        }
        
        row.populate("A", 0, -200, undefined);
        const initialHeight = row.element.height();
        let charactersInline = 1;
        
        while (true) {
            row.populate(_.repeat("A", charactersInline), 0, -200, undefined);
            if (row.element.height() > initialHeight) {
                break;
            }
            charactersInline++;
        }
        
        charactersInline -= 3; // substract few character to have extra space for scrolls
        
        const doubleLinesHeight = row.element.height();
        
        this.lineHeight = doubleLinesHeight - initialHeight;
        this.padding = doubleLinesHeight - 2 * this.lineHeight;
        this.charactersPerLine = charactersInline;
    }
}

class adminLogs extends viewModelBase {

    private liveClient = ko.observable<adminLogsWebSocketClient>();
    private listController = ko.observable<listViewController<string>>();
    private headerSeen = false;
    
    private allData = [] as string[];
    
    filter = ko.observable<string>("");
    onlyErrors = ko.observable<boolean>(false);
    
    private appendElementsTask: number;
    private pendingMessages = [] as string[];
    private heightCalculator = new heightCalculator();
    
    private configuration = ko.observable<adminLogsConfig>(adminLogsConfig.empty());
    
    editedConfiguration = ko.observable<adminLogsConfig>(adminLogsConfig.empty());
    editedSourceName = ko.observable<string>();
    
    isBufferFull = ko.observable<boolean>();
    
    tailEnabled = ko.observable<boolean>(true);
    private duringManualScrollEvent = false;

    validationGroup: KnockoutValidationGroup;
    enableApply: KnockoutComputed<boolean>;
    
    constructor() {
        super();
        
        this.bindToCurrentInstance("toggleTail", "itemHeightProvider", "applyConfiguration", "includeSource", "excludeSource", "removeConfigurationEntry", "itemHtmlProvider");
        this.filter.throttle(500).subscribe(() => this.filterLogEntries(true));
        this.onlyErrors.subscribe(() => this.filterLogEntries(true));
        this.initValidation(); 
    }
    
    private initValidation() {
        this.editedConfiguration().maxEntries.extend({
            required: true,
            min: 0
        });

        this.validationGroup = ko.validatedObservable({
            maxEntries: this.editedConfiguration().maxEntries
        });

        this.enableApply = ko.pureComputed(() => {
            return this.isValid(this.validationGroup);
        });
    }
    
    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('57BGF7');
    }
    
    deactivate() {
        super.deactivate();
        
        if (this.liveClient()) {
            this.liveClient().dispose();
        }
    }

    filterLogEntries(fromFilterChange: boolean) {
        const searchText = this.filter().toLocaleLowerCase();
        const errorsOnly = this.onlyErrors();

        if (fromFilterChange) {
            this.listController().reset();
        }

        if (searchText || errorsOnly) {
            let filterFunction: (item: string) => boolean = null;
            if (searchText && errorsOnly) {
                filterFunction = x => x.toLocaleLowerCase().includes(searchText) && this.hasError(x);
            } else if (searchText) {
                filterFunction = x => x.toLocaleLowerCase().includes(searchText);
            } else {
                filterFunction = x => this.hasError(x);
            }
            
            const filteredItems = fromFilterChange 
                ? this.allData.filter(filterFunction)
                : this.pendingMessages.filter(filterFunction);
            
            this.listController().pushElements(filteredItems);
        } else {
            this.listController().pushElements(fromFilterChange ? this.allData : this.pendingMessages);
        }
    }

    applyConfiguration() {
        if (this.isValid(this.validationGroup)) {
            
            this.editedConfiguration().copyTo(this.configuration());

            // restart websocket
            this.isBufferFull(false);
            this.pauseLogs();
            this.resumeLogs();
        }
    }
    
    itemHeightProvider(item: string, row: virtualListRow<string>) {
        return this.heightCalculator.measure(item, row);
    }
    
    private hasError(item: string): boolean {
        return item.includes("EXCEPTION:") || item.includes("Exception:") || item.includes("FATAL ERROR:");
    }    
    
    // noinspection JSMethodCanBeStatic
    itemHtmlProvider(item: string) {
        return $("<pre class='item'></pre>")
            .toggleClass("bg-danger", this.hasError(item))
            .text(item);
    }
    
    compositionComplete() {
        super.compositionComplete();
        this.connectWebSocket();
        
        $(".admin-logs .viewport").on("scroll", () => {
            if (!this.duringManualScrollEvent && this.tailEnabled()) {
                this.tailEnabled(false);
            }
            
            this.duringManualScrollEvent = false;
        });
    }
    
    connectWebSocket() {
        eventsCollector.default.reportEvent("admin-logs", "connect");
        const ws = new adminLogsWebSocketClient(this.configuration(), data => this.onData(data));
        this.liveClient(ws);
        
        this.headerSeen = false;
    }
    
    pauseLogs() {
        eventsCollector.default.reportEvent("admin-logs", "pause");
        if (this.liveClient()) {
            this.liveClient().dispose();
            this.liveClient(null);
        }
    }
    
    resumeLogs() {
        this.connectWebSocket();
    }
    
    private onData(data: string) {
        if (this.listController().getTotalCount() + this.pendingMessages.length >= this.configuration().maxEntries()) {
            this.isBufferFull(true);
            this.pauseLogs();
            return;
        }
        
        data = data.trim();
        
        if (!this.headerSeen) {
            
            this.headerSeen = true;
            return;
        }
        
        this.allData.push(data);
        this.pendingMessages.push(data);
        
        if (!this.appendElementsTask) {
            this.appendElementsTask = setTimeout(() => this.onAppendPendingMessages(), 333);
        }
    }
    
    private onAppendPendingMessages() {
        this.appendElementsTask = null;

        this.filterLogEntries(false);

        this.pendingMessages.length = 0;

        if (this.tailEnabled()) {
            this.scrollDown();
        }
    }
    
    clear() {
        eventsCollector.default.reportEvent("admin-logs", "clear");
        this.allData.length = 0;
        this.isBufferFull(false);
        this.listController().reset();
        
        // set flag to true, since list reset is async
        this.duringManualScrollEvent = true;
        this.tailEnabled(true);
        
        if (!this.liveClient()) {
            this.resumeLogs();
        }
    }
    
    exportToFile() {
        eventsCollector.default.reportEvent("admin-logs", "export");
        const items = this.listController().getItems();
        const lines = [] as string[];
        items.forEach(v => {
            lines.push(v);
        });
        
        const joinedFile = lines.join("\r\n");
        const now = moment().format("YYYY-MM-DD HH-mm");
        fileDownloader.downloadAsTxt(joinedFile, "admin-log-" + now + ".txt");
    }
    
    toggleTail() {
        this.tailEnabled.toggle();
        
        if (this.tailEnabled()) {
            this.scrollDown();
        }
    }
    
    private scrollDown() {
        this.duringManualScrollEvent = true;

        this.listController().scrollDown();
    }

    includeSource() {
        const source = this.editedSourceName();
        if (source) {
            const configItem = new adminLogsConfigEntry(source, "include");
            this.editedConfiguration().entries.unshift(configItem);
            this.editedSourceName("");
        }
    }
    
    excludeSource() {
        const source = this.editedSourceName();
        if (source) {
            const configItem = new adminLogsConfigEntry(source, "exclude");
            this.editedConfiguration().entries.push(configItem);
            this.editedSourceName("");
        }
    }

    removeConfigurationEntry(entry: adminLogsConfigEntry) {
        this.editedConfiguration().entries.remove(entry);
    }

    onOpenOptions() {
        this.editedConfiguration().maxEntries(this.configuration().maxEntries());
    }
}

export = adminLogs;
