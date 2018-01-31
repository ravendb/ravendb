import viewModelBase = require("viewmodels/viewModelBase");

import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import columnsSelector = require("viewmodels/partial/columnsSelector");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import getDebugThreadsRunawayCommand = require("commands/database/debug/getDebugThreadsRunawayCommand");

class debugAdvancedThreadsRuntime extends viewModelBase {

    data = ko.observable<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo[]>();

    private gridController = ko.observable<virtualGridController<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>>();
    columnsSelector = new columnsSelector<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>();
    private columnPreview = new columnPreviewPlugin<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>();

    spinners = {
        refresh: ko.observable<boolean>(false),
    };

    activate(args: any) {
        super.activate(args);

        return this.loadThreadsRunaway();
    }

    compositionComplete(): void {
        super.compositionComplete();

        const fetcher = () => {
            const data = this.data() || [];

            return $.when({
                totalResultCount: data.length,
                items: data
            } as pagedResult<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>);
        };

        const grid = this.gridController();
        grid.headerVisible(true);
        grid.init(fetcher, () =>
            [
                new textColumn<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>(grid, x => x.Name, "Name", "25%"),
                new textColumn<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>(grid, x => (x.ManagedThreadId || 'n/a') + " (" + x.Id + ")", "Thread Id", "10%"),
                new textColumn<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>(grid, x => x.StartingTime, "Start Time", "20%"),
                new textColumn<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>(grid, x => x.Duration, "Duration", "10%"),
                new textColumn<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>(grid, x => x.State, "State", "10%"),
                new textColumn<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>(grid, x => x.WaitReason, "Wait reason", "20%"),
            ]
        );

        this.columnPreview.install("virtual-grid", ".tooltip",
            (entry: Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo,
             column: textColumn<Raven.Server.Documents.Handlers.Debugging.ThreadsHandler.ThreadInfo>,
             e: JQueryEventObject, onValue: (context: any) => void) => {
                if (column.header === "Duration") {
                    const timings = {
                        StartTime: entry.StartingTime,
                        TotalProcessorTime: entry.TotalProcessorTime,
                        PrivilegedProcessorTime: entry.PrivilegedProcessorTime,
                        UserProcessorTime: entry.UserProcessorTime
                    };
                    const json = JSON.stringify(timings, null, 4);
                    const html = Prism.highlight(json, (Prism.languages as any).javascript);
                    onValue(html);
                } else {
                    const value = column.getCellValue(entry);
                    onValue(value);
                }
            });
    }

    private loadThreadsRunaway() {
        return new getDebugThreadsRunawayCommand()
            .execute()
            .done(response => {
                this.data(response);
            });
    }

    refresh() {
        this.spinners.refresh(true);
        return this.loadThreadsRunaway()
            .done(() => this.gridController().reset(true))
            .always(() => this.spinners.refresh(false));
    }

}

export = debugAdvancedThreadsRuntime;
