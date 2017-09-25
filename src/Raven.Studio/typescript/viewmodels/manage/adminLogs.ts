import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import adminLogsWebSocketClient = require("common/adminLogsWebSocketClient");
import adminLogsConfig = require("models/database/debug/adminLogsConfig");
import adminLogsConfigEntry = require("models/database/debug/adminLogsConfigEntry");
import eventsCollector = require("common/eventsCollector");
import listViewController = require("widgets/listView/listViewController");
import fileDownloader = require("common/fileDownloader");

class adminLogs extends viewModelBase {

    private liveClient = ko.observable<adminLogsWebSocketClient>();
    private listController = ko.observable<listViewController<string>>();
    private headerSeen = false;
    
    private configuration = ko.observable<adminLogsConfig>(adminLogsConfig.empty());
    
    tailEnabled = ko.observable<boolean>(true);

    constructor() {
        super();
        
        this.bindToCurrentInstance("toggleTail");
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

    itemHeightProvider(item: string) {
        return 27 + 17 * (item.split("\r\n").length - 1);  //tODO:
    }
    
    itemHtmlProvider(item: string) {
        const itemLower = item.toLocaleLowerCase();
        const hasError = itemLower.includes("exception") || itemLower.includes("error") || itemLower.includes("failure");
        
        return $("<pre class='item'></pre>")
            .toggleClass("bg-danger", hasError)
            .html(item);
    }
    
    compositionComplete() {
        super.compositionComplete();
        this.connectWebSocket();
    }
    
    connectWebSocket() {
        eventsCollector.default.reportEvent("admin-logs", "connect");
        const ws = new adminLogsWebSocketClient(null, this.configuration(), data => this.onData(data));
        this.liveClient(ws);
        
        this.headerSeen = false;
    }
    
    pauseLogs() {
        eventsCollector.default.reportEvent("admin-logs", "pause");
        this.liveClient().dispose();
        this.liveClient(null);
    }
    
    resumeLogs() {
        this.connectWebSocket();
    }
    
    private onData(data: string) {
        //TODO: is no space in buffer then disconnect!
        
        if (!this.headerSeen) {
            
            this.headerSeen = true;
            return;
        }
        
        this.listController().pushElements([data.trim()]);
        
        if (this.tailEnabled()) {
            this.listController().scrollDown();
        }
    }
    
    clear() {
        eventsCollector.default.reportEvent("admin-logs", "clear");
        this.listController().reset();
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
            this.listController().scrollDown();
        }
    }
}

export = adminLogs;
