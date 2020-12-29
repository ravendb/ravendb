import viewModelBase = require("viewmodels/viewModelBase");
import adminLogsWebSocketClient = require("common/adminLogsWebSocketClient");
import adminLogsConfig = require("models/database/debug/adminLogsConfig");
import adminLogsConfigEntry = require("models/database/debug/adminLogsConfigEntry");
import eventsCollector = require("common/eventsCollector");
import listViewController = require("widgets/listView/listViewController");
import fileDownloader = require("common/fileDownloader");
import virtualListRow = require("widgets/listView/virtualListRow");
import copyToClipboard = require("common/copyToClipboard");
import generalUtils = require("common/generalUtils");
import getAdminLogsConfigurationCommand = require("commands/maintenance/getAdminLogsConfigurationCommand");
import saveAdminLogsConfigurationCommand = require("commands/maintenance/saveAdminLogsConfigurationCommand");
import adminLogsOnDiskConfig = require("models/database/debug/adminLogsOnDiskConfig");

class heightCalculator {
    
    private charactersPerLine = undefined as number;
    private padding = undefined as number;
    private lineHeight = undefined as number;
    
    measure(item: string, row: virtualListRow<string>) {
        this.ensureCacheFilled(row);
        
        const lines = item.split(/\r?\n/);
        const totalLinesCount = _.sum(lines.map(l => {
            if (l.length > this.charactersPerLine) {
                return Math.ceil(l.length / this.charactersPerLine);
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
        
        charactersInline -= 3; // subtract few character to have extra space for scrolls
        
        const doubleLinesHeight = row.element.height();
        
        this.lineHeight = doubleLinesHeight - initialHeight;
        this.padding = doubleLinesHeight - 2 * this.lineHeight;
        this.charactersPerLine = charactersInline;
        
        row.populate("", 0, -200, undefined);
    }
}

class adminLogs extends viewModelBase {

    private liveClient = ko.observable<adminLogsWebSocketClient>();
    private listController = ko.observable<listViewController<string>>();
    
    private allData = [] as string[];
    
    filter = ko.observable<string>("");
    onlyErrors = ko.observable<boolean>(false);
    mouseDown = ko.observable<boolean>(false);

    headerValuePlaceholder: KnockoutComputed<string>;
    
    private appendElementsTask: number;
    private pendingMessages = [] as string[];
    private heightCalculator = new heightCalculator();
    
    private onDiskConfiguration = ko.observable<adminLogsOnDiskConfig>();
    private configuration = ko.observable<adminLogsConfig>(adminLogsConfig.empty());
    
    editedConfiguration = ko.observable<adminLogsConfig>(adminLogsConfig.empty());
    editedHeaderName = ko.observable<adminLogsHeaderType>("Source");
    editedHeaderValue = ko.observable<string>();
    
    isBufferFull = ko.observable<boolean>();
    
    tailEnabled = ko.observable<boolean>(true);
    private duringManualScrollEvent = false;

    validationGroup: KnockoutValidationGroup;
    enableApply: KnockoutComputed<boolean>;

    isPauseLogs = ko.observable<boolean>(false);
    connectionJustOpened = ko.observable<boolean>(false);
    
    private static readonly studioMsgPart = "-, Information, Studio,";
    
    constructor() {
        super();
        
        this.bindToCurrentInstance("toggleTail", "itemHeightProvider", "applyConfiguration", "loadLogsConfig",
            "includeFilter", "excludeFilter", "removeConfigurationEntry", "itemHtmlProvider", "setAdminLogMode");
        
        this.initObservables();
        this.initValidation();
    }
    
    private initObservables() {
        this.filter.throttle(500).subscribe(() => this.filterLogEntries(true));
        this.onlyErrors.subscribe(() => this.filterLogEntries(true));

        this.enableApply = ko.pureComputed(() => {
            return this.isValid(this.validationGroup);
        });

        this.headerValuePlaceholder = ko.pureComputed(() => {
            switch (this.editedHeaderName()) {
                case "Source":
                    return "Source name (ex. Server, Northwind, Orders/ByName)";
                case "Logger":
                    return "Logger name (ex. Raven.Server.Documents.)"
            }
        });
        
        this.mouseDown.subscribe(pressed => {
            if (!pressed) {
                const selected = generalUtils.getSelectedText();
                if (selected) {
                    copyToClipboard.copy(selected, "Selected logs has been copied to clipboard");
                }
            }
        });
    }
    
    private initValidation() {
        this.editedConfiguration().maxEntries.extend({
            required: true,
            min: 0
        });

        this.validationGroup = ko.validatedObservable({
            maxEntries: this.editedConfiguration().maxEntries
        });
    }
    
    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('57BGF7');
        
        return this.loadLogsConfig(); 
    }
    
    deactivate() {
        super.deactivate();
        
        if (this.liveClient()) {
            this.liveClient().dispose();
        }
    }
    
    loadLogsConfig() {
        return new getAdminLogsConfigurationCommand().execute()
            .done(result => this.onDiskConfiguration(new adminLogsOnDiskConfig(result)));
    }

    setAdminLogMode(newMode: Sparrow.Logging.LogMode) {
        this.onDiskConfiguration().selectedLogMode(newMode);

        // First must get updated with current server settings
        new getAdminLogsConfigurationCommand().execute()
            .done((result) => {
                const config = new adminLogsOnDiskConfig(result);
                config.selectedLogMode(newMode);
            
                // Set the new mode
                new saveAdminLogsConfigurationCommand(config).execute()
                    .always(this.loadLogsConfig);
        });
    }
    
    filterLogEntries(fromFilterChange: boolean) {
        if (fromFilterChange) {
            this.listController().reset();
        }

        const filterFunction = this.getFilterFunction();

        const itemsToPush = fromFilterChange ?
            (filterFunction ? this.allData.filter(filterFunction) : this.allData) :
            (filterFunction ? this.pendingMessages.filter(filterFunction) : this.pendingMessages);

        this.listController().pushElements(itemsToPush);
    }
    
    getFilterFunction(): (item: string) => boolean {
        const searchText = this.filter().toLocaleLowerCase();
        const errorsOnly = this.onlyErrors();

        if (searchText || errorsOnly) {
            if (searchText && errorsOnly) {
                return x => (x.toLocaleLowerCase().includes(searchText) && this.hasError(x)) || this.isStudioItem(x);
            } else if (searchText) {
                return x => x.toLocaleLowerCase().includes(searchText) || this.isStudioItem(x);
            } else {
                return x => this.hasError(x) || this.isStudioItem(x);
            }
        }
        return null;
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
    
    private isStudioItem(item: string): boolean {
        return item.includes(adminLogs.studioMsgPart);
    }
    
    private getAddedClass(item: string) {
        if (this.hasError(item)) {
            return "bg-danger";
        }
        
        const isStudioItem = this.isStudioItem(item);
        if (isStudioItem && item.includes("Connection interrupted by server")) {
            return "text-danger";
        }
        
        if (isStudioItem) {
            return "studio-item-info";
        }
        
        return "";
    } 

    // noinspection JSMethodCanBeStatic
    itemHtmlProvider(item: string) {
        const addedClass = this.getAddedClass(item);
        const addedClassHtml = addedClass ? `class="${addedClass}"` : "";

        return $(`<pre class="item"></pre>`)
            .addClass("flex-horizontal")
            .prepend(`<span ${addedClassHtml}>${generalUtils.escapeHtml(item)}</span>`)
            .prepend(`<a href="#" class="copy-item-button margin-right margin-right-sm flex-start" title="Copy log msg to clipboard"><i class="icon-copy"></i></a>`);
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
      
        $(".list-view").on("click", ".copy-item-button", function(event) {
            event.preventDefault();
            event.stopImmediatePropagation();
            copyToClipboard.copy($(this).next().text(), "Log message has been copied to clipboard");
        });
    }
    
    connectWebSocket() {
        eventsCollector.default.reportEvent("admin-logs", "connect");
        const ws = new adminLogsWebSocketClient(this.configuration(), data => this.onData(data));
        this.liveClient(ws);

        this.liveClient().isConnected.subscribe((opened) => {
            if (opened) {
                this.connectionJustOpened(opened);
            } else {
                const customMsg = this.isPauseLogs() ? "Connection paused" : "Connection interrupted by server";
                this.addStudioMessage(customMsg);
            }
        });
    }

    isConnectedToWebSocket() {
        if (this.liveClient() && this.liveClient().isConnected()) {
            return true;
        }
        return false;
    }
    
    pauseLogs() {
        eventsCollector.default.reportEvent("admin-logs", "pause");
        
        if (this.liveClient()) {
            this.liveClient().dispose();
            this.liveClient(null);
            this.isPauseLogs(true);
        }
    }
    
    resumeLogs() {
        this.connectWebSocket();
        this.isPauseLogs(false);
    }
    
    private onData(data: string) {
        if (this.listController().getTotalCount() + this.pendingMessages.length >= this.configuration().maxEntries()) {
            this.isBufferFull(true);
            this.pauseLogs();
            return;
        }

        if (this.connectionJustOpened()) {
            this.connectionJustOpened(false);
            // replace the initial 'headers' msg
            this.addStudioMessage("Connection established");
        } else {
            this.addMessage(data.trim());
        }

        if (!this.appendElementsTask) {
            this.appendElementsTask = setTimeout(() => this.onMessageAdded(), 333);
        }
    }

    private addStudioMessage(msg: string) {
        const time = new Date().toISOString();
        msg = `${time.replace("Z", "0000Z")}, ${adminLogs.studioMsgPart} ${msg}`;
        this.addMessage(msg, true);
    }
    
    private addMessage(msg: string, showMessageNow: boolean = false) {
        this.allData.push(msg);
        
        if (showMessageNow) {
            const filterFunction = this.getFilterFunction();
            const itemsToPush = filterFunction ? this.pendingMessages.filter(this.getFilterFunction()) : this.pendingMessages;
            
            this.listController().pushElements([...itemsToPush, msg]);
            this.pendingMessages.length = 0;
            
        } else {
            this.pendingMessages.push(msg);
        }
    }
    
    private onMessageAdded() {
        if (this.mouseDown()) {
            // looks like user wants to select something - wait with updates 
            this.appendElementsTask = setTimeout(() => this.onMessageAdded(), 700);
            return;
        }
        
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

    includeFilter() {
        const headerName = this.editedHeaderName();
        const headerValue = this.editedHeaderValue();
        if (headerName && headerValue) {
            const configItem = new adminLogsConfigEntry(headerName, headerValue, "include");
            this.editedConfiguration().entries.unshift(configItem);
            this.resetFiltersForm();
        }
    }
    
    excludeFilter() {
        const headerName = this.editedHeaderName();
        const headerValue = this.editedHeaderValue();
        if (headerName && headerValue) {
            const configItem = new adminLogsConfigEntry(headerName, headerValue, "exclude");
            this.editedConfiguration().entries.push(configItem);
            this.resetFiltersForm();
        }
    }
    
    private resetFiltersForm() {
        this.editedHeaderName("Source");
        this.editedHeaderValue("");
    }

    removeConfigurationEntry(entry: adminLogsConfigEntry) {
        this.editedConfiguration().entries.remove(entry);
    }

    onOpenOptions() {
        this.editedConfiguration().maxEntries(this.configuration().maxEntries());
    }

    onOpenSettings() {
        this.loadLogsConfig();
    }
    
    updateMouseStatus(pressed: boolean) {
        this.mouseDown(pressed);
        return true;  // we want bubble and execute default action (selection)
    }
}

export = adminLogs;
