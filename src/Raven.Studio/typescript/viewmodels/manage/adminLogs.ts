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
import datePickerBindingHandler = require("common/bindingHelpers/datePickerBindingHandler");
import adminLogsOnDiskConfig = require("models/database/debug/adminLogsOnDiskConfig");
import moment = require("moment");
import endpoints = require("endpoints");
import appUrl = require("common/appUrl");
import adminLogsTrafficWatchDialog = require("./adminLogsTrafficWatchDialog");
import app = require("durandal/app");
import getTrafficWatchConfigurationCommand = require("commands/maintenance/getTrafficWatchConfigurationCommand");
import trafficWatchConfiguration = require("models/resources/trafficWatchConfiguration");
import saveTrafficWatchConfigurationCommand = require("commands/maintenance/saveTrafficWatchConfigurationCommand");
import getAdminLogsMicrosoftStateCommand = require("commands/maintenance/getAdminLogsMicrosoftStateCommand");
import getAdminLogsMicrosoftConfigurationCommand = require("commands/maintenance/getAdminLogsMicrosoftConfigurationCommand");
import enableAdminLogsMicrosoftCommand = require("commands/maintenance/enableAdminLogsMicrosoftCommand");
import disableAdminLogsMicrosoftCommand = require("commands/maintenance/disableAdminLogsMicrosoftCommand");
import saveAdminLogsMicrosoftConfigurationCommand = require("commands/maintenance/saveAdminLogsMicrosoftConfigurationCommand");
import configureMicrosoftLogsDialog = require("./configureMicrosoftLogsDialog");
import getAdminLogsEventListenerConfigurationCommand = require("commands/maintenance/getAdminLogsEventListenerConfigurationCommand");
import saveAdminLogsEventListenerConfigurationCommand = require("commands/maintenance/saveAdminLogsEventListenerConfigurationCommand");
import configureEventListenerDialog = require("viewmodels/manage/configureEventListenerDialog");

type EventListenerConfigurationDto = Omit<Raven.Server.EventListener.EventListenerToLog.EventListenerConfiguration, "Persist">;

class heightCalculator {
    
    private charactersPerLine: number = undefined;
    private padding: number = undefined;
    private lineHeight: number = undefined;
    
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
        
        // eslint-disable-next-line no-constant-condition
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

    view = require("views/manage/adminLogs.html");

    private liveClient = ko.observable<adminLogsWebSocketClient>();
    private listController = ko.observable<listViewController<string>>();
    
    private allData: string[] = [];
    
    filter = ko.observable<string>("");
    onlyErrors = ko.observable<boolean>(false);
    mouseDown = ko.observable<boolean>(false);

    headerValuePlaceholder: KnockoutComputed<string>;
    
    private appendTask: number;
    private pendingMessages: string[] = [];
    private heightCalculator = new heightCalculator();
    
    private onDiskConfiguration = ko.observable<adminLogsOnDiskConfig>();
    private configuration = ko.observable<adminLogsConfig>(adminLogsConfig.empty());
    private trafficWatchConfiguration = ko.observable<trafficWatchConfiguration>();

    isMicrosoftLogsEnabled = ko.observable<boolean>(false);
    microsoftLogsConfiguration = ko.observable<string>("");

    private eventListenerConfiguration = ko.observable<EventListenerConfigurationDto>();
    
    editedConfiguration = ko.observable<adminLogsConfig>(adminLogsConfig.empty());
    editedHeaderName = ko.observable<adminLogsHeaderType>("Source");
    editedHeaderValue = ko.observable<string>();
    
    isBufferFull = ko.observable<boolean>();
    
    tailEnabled = ko.observable<boolean>(true);
    private duringManualScrollEvent = false;

    validationGroup: KnockoutValidationGroup;
    downloadLogsValidationGroup: KnockoutValidationGroup;
    
    enableApply: KnockoutComputed<boolean>;

    isPauseLogs = ko.observable<boolean>(false);
    connectionJustOpened = ko.observable<boolean>(false);
    
    private static readonly studioMsgPart = "-, Information, Studio,";
    
    datePickerOptions = {
        format: "YYYY-MM-DD HH:mm:ss.SSS",
        sideBySide: true
    };

    useMinStartDate = ko.observable<boolean>(false);
    startDate = ko.observable<moment.Moment>();

    useMaxEndDate = ko.observable<boolean>(false);
    endDate = ko.observable<moment.Moment>();

    startDateToUse: KnockoutComputed<string>;
    endDateToUse: KnockoutComputed<string>;

    trafficWatchEnabled: KnockoutComputed<boolean>;

    // show user location of traffic watch in logs configuration button
    highlightTrafficWatch: boolean;

    static utcTimeFormat = "YYYY-MM-DD HH:mm:ss.SSS";
    
    constructor() {
        super();
        
        this.bindToCurrentInstance("toggleTail", "itemHeightProvider", "applyConfiguration", "loadLogsConfig",
            "includeFilter", "excludeFilter", "removeConfigurationEntry", "itemHtmlProvider", "setAdminLogMode", 
            "configureTrafficWatch", "configureMicrosoftLogs", "configureEventListener");
        
        this.initObservables();
        this.initValidation();

        datePickerBindingHandler.install();
    }
    
    private initObservables() {
        this.filter.throttle(500).subscribe(() => this.filterAndAppendLogEntries(true));
        this.onlyErrors.subscribe(() => this.filterAndAppendLogEntries(true));

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

        this.startDateToUse = ko.pureComputed(() => {
            return this.useMinStartDate() ? null : this.startDate().utc().format(generalUtils.utcFullDateFormat);
        });

        this.endDateToUse = ko.pureComputed(() => {
            return this.useMaxEndDate() ? null : this.endDate().utc().format(generalUtils.utcFullDateFormat);
        });
        
        this.trafficWatchEnabled = ko.pureComputed(() => {
            const config = this.trafficWatchConfiguration();
            return config?.enabled() ?? false;
        });
    }
    
    dateFormattedAsUtc(localDate: moment.Moment) {
        if (localDate) {
            const date = moment(localDate);
            if (!date.isValid()) {
                return "";
            }
            return date.utc().format(adminLogs.utcTimeFormat) + "Z (UTC)";
        } else {
            return "";
        }
    }
    
    private initValidation() {
        this.editedConfiguration().maxEntries.extend({
            required: true,
            min: 0
        });

        this.startDate.extend({
            required: {
                onlyIf: () => !this.useMinStartDate()
            },
            validation: [
                {
                    validator: () => {
                        if (this.useMinStartDate()) {
                            return true;
                        }
                        return this.startDate().isValid();
                    },
                    message: "Please enter a valid date"
                }
            ]
        });

        this.endDate.extend({
            required: {
                onlyIf: () => !this.useMaxEndDate()
            },
            validation: [
                {
                    validator: () => {
                        if (this.useMaxEndDate()) {
                            return true;
                        }
                        return this.endDate().isValid();
                    },
                    message: "Please enter a valid date"
                },
                {
                    validator: () => {
                        if (this.useMaxEndDate() || this.useMinStartDate()) {
                            return true;
                        }

                        if (!this.startDate() || !this.startDate().isValid()) {
                            return true;
                        }

                        // at this point both start/end are defined and valid, we can compare
                        return this.endDate().diff(this.startDate()) >= 0;
                    },
                    message: "End Date must be greater than Start Date"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            maxEntries: this.editedConfiguration().maxEntries
        });
        
        this.downloadLogsValidationGroup = ko.validatedObservable({
            startDate: this.startDate,
            endDate: this.endDate
        });
    }
    
    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('57BGF7');

        this.highlightTrafficWatch = !!args?.highlightTrafficWatch;
        
        return this.loadConfigs();
    }
    
    deactivate() {
        super.deactivate();
        
        if (this.liveClient()) {
            this.liveClient().dispose();
        }
    }
    
    loadConfigs() {
        const logConfigsTask = this.loadLogsConfig();
        const trafficWatchConfigTask = this.loadTrafficWatchConfig();
        const loadIsMicrosoftLogsEnabledTask = this.loadIsMicrosoftLogsEnabled();
        const loadMicrosoftLogsConfigurationTask = this.loadMicrosoftLogsConfiguration();
        const loadEventListenerConfigurationTask = this.loadEventListenerConfiguration();
        
        return $.when<any>(logConfigsTask, trafficWatchConfigTask, loadIsMicrosoftLogsEnabledTask, loadMicrosoftLogsConfigurationTask, loadEventListenerConfigurationTask);
    }
    
    loadLogsConfig() {
        return new getAdminLogsConfigurationCommand().execute()
            .done(result => this.onDiskConfiguration(new adminLogsOnDiskConfig(result)));
    }
    
    loadTrafficWatchConfig() {
        return new getTrafficWatchConfigurationCommand().execute()
            .done(result => this.trafficWatchConfiguration(new trafficWatchConfiguration(result)))
    }
    
    loadIsMicrosoftLogsEnabled() {
        return new getAdminLogsMicrosoftStateCommand().execute()
            .done(result => this.isMicrosoftLogsEnabled(result.IsActive));
    }

    loadMicrosoftLogsConfiguration() {
        return new getAdminLogsMicrosoftConfigurationCommand().execute()
            .done(result => this.microsoftLogsConfiguration(JSON.stringify(result, null, 4)));
    }

    loadEventListenerConfiguration() {
        return new getAdminLogsEventListenerConfigurationCommand().execute()
            .done(result => this.eventListenerConfiguration(result));
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
    
    filterAndAppendLogEntries(fromFilterChange: boolean) {
        if (fromFilterChange) {
            this.listController().reset();
        }

        let itemsToPush = fromFilterChange ? this.allData : this.pendingMessages;
        
        const filterFunction = this.getFilterFunction();
        if (filterFunction) {
            itemsToPush =  itemsToPush.filter(filterFunction)
        }
        
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
        
        
        if (this.highlightTrafficWatch) {
            this.showTrafficWatchConfigurationLocation();
        }
        
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
    
    private showTrafficWatchConfigurationLocation() {
        setTimeout(() => {
            $("#js-settings-btn").click();
            
            const blink = () => {
                const element = $("#js-traffic-watch-config");
                element.removeClass("blink-style");
                setTimeout(() => element.addClass("blink-style"), 1);
            }
            
            setTimeout(blink, 1000);
            setTimeout(blink, 2000);
        }, 400);
        
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
        return this.liveClient() && this.liveClient().isConnected();
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
    }

    private addStudioMessage(msg: string) {
        const time = new Date().toISOString();
        msg = `${time.replace("Z", "0000Z")}, ${adminLogs.studioMsgPart} ${msg}`;
        this.addMessage(msg);
    }
    
    private addMessage(msg: string) {
        this.allData.push(msg);
        this.pendingMessages.push(msg);
        
        this.scheduleAppendTask();
    }
    
    private scheduleAppendTask() {
        if (!this.appendTask) {
            this.appendTask = setTimeout(() => this.appendPendingMessages(), 333);
        }
    }
    
    private appendPendingMessages() {
        if (this.mouseDown()) {
            // looks like user wants to select something - wait with updates 
            this.appendTask = setTimeout(() => this.appendPendingMessages(), 700);
            return;
        }
        
        this.appendTask = null;
        
        this.filterAndAppendLogEntries(false);

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
    }
    
    exportToFile() {
        eventsCollector.default.reportEvent("admin-logs", "export");
        const items = this.listController().getItems();
        const lines: string[] = [];
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
    
    onOpenDownload() {
        this.startDate(null);
        this.endDate(null);
        
        this.useMinStartDate(false);
        this.useMaxEndDate(false);
        
        this.downloadLogsValidationGroup.errors.showAllMessages(false);
    }

    onDownloadLogs() {
        if (!this.isValid(this.downloadLogsValidationGroup)) {
            return;
        }

        const $form = $("#downloadLogsForm");
        const url = endpoints.global.adminLogs.adminLogsDownload;
        
        $form.attr("action", appUrl.forServer() + url);

        $("[name=from]", $form).val(this.startDateToUse());
        $("[name=to]", $form).val(this.endDateToUse());

        $form.submit();
    }
    
    updateMouseStatus(pressed: boolean) {
        this.mouseDown(pressed);
        return true;  // we want bubble and execute default action (selection)
    }
    
    configureTrafficWatch() {
        const configurationCopy = new trafficWatchConfiguration(this.trafficWatchConfiguration().toDto());

        app.showBootstrapDialog(new adminLogsTrafficWatchDialog(configurationCopy))
            .done(result => {
                if (result) {
                    this.trafficWatchConfiguration(result);
                    new saveTrafficWatchConfigurationCommand(this.trafficWatchConfiguration().toDto())
                        .execute();
                }
            });
    }

    configureMicrosoftLogs() {
        app.showBootstrapDialog(new configureMicrosoftLogsDialog(this.isMicrosoftLogsEnabled(), this.microsoftLogsConfiguration()))
            .done((result: ConfigureMicrosoftLogsDialogResult) => {
                if (!result) {
                    return;
                }
                
                if (this.isMicrosoftLogsEnabled() !== result.isEnabled) { 
                    if (result.isEnabled) {
                        new enableAdminLogsMicrosoftCommand()
                            .execute()
                            .done(() => this.loadIsMicrosoftLogsEnabled());
                    } else {
                        new disableAdminLogsMicrosoftCommand()
                            .execute()
                            .done(() => this.loadIsMicrosoftLogsEnabled());
                    }
                    
                }
                
                if (result.configuration) {
                    new saveAdminLogsMicrosoftConfigurationCommand(result.configuration, result.persist)
                        .execute()
                        .done(() => this.loadMicrosoftLogsConfiguration());
                }
            });
    }

    configureEventListener() {
        app.showBootstrapDialog(new configureEventListenerDialog(this.eventListenerConfiguration()))
            .done((config: Raven.Server.EventListener.EventListenerToLog.EventListenerConfiguration) => {
                if (config) {
                    new saveAdminLogsEventListenerConfigurationCommand(config)
                        .execute()
                        .done(() => this.loadEventListenerConfiguration());
                }    
            });
    }
}

export = adminLogs;
