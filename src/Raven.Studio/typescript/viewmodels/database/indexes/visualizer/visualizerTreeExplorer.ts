import graphHelper = require("common/helpers/graph/graphHelper");
import viewHelpers = require("common/helpers/view/viewHelpers");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");


import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

class visualizerTreeExplorer extends dialogViewModelBase {

    private tableItems = [] as Raven.Server.Documents.Indexes.Debugging.MapResultInLeaf[];
    private gridController = ko.observable<virtualGridController<Raven.Server.Documents.Indexes.Debugging.MapResultInLeaf>>();
    private columnPreview = new columnPreviewPlugin<Raven.Server.Documents.Indexes.Debugging.MapResultInLeaf>();

    private dto: Raven.Server.Documents.Indexes.Debugging.ReduceTreePage;
    private aggregationResult = ko.observable<string>();
    private hasEntries: boolean;

    constructor(dto: Raven.Server.Documents.Indexes.Debugging.ReduceTreePage) {
        super();
        this.tableItems = dto.Entries;
        this.aggregationResult(JSON.stringify(dto.AggregationResult, null, 4));
        this.hasEntries = !!dto.Entries;

        aceEditorBindingHandler.install();
    }

    compositionComplete() {
        super.compositionComplete();

        if (this.hasEntries) {
            const grid = this.gridController();
            grid.headerVisible(true);

            grid.init((s, t) => this.fetcher(s, t), () => this.findColumns());

            this.columnPreview.install(".visualiserTreeExplorer", ".js-visualizer-tree-tooltip",
                (details: Raven.Server.Documents.Indexes.Debugging.MapResultInLeaf, column: textColumn<Raven.Server.Documents.Indexes.Debugging.MapResultInLeaf>,
                 e: JQueryEventObject, onValue: (context: any, valueToCopy: string) => void) => {
                    const value = column.getCellValue(details);
                    if (!_.isUndefined(value)) {
                        const json = JSON.stringify(value, null, 4);
                        const html = Prism.highlight(json, (Prism.languages as any).javascript);
                        onValue(html, json);
                    }
                });
        }
    }

    private findColumns() {
        const keys = Object.keys(this.tableItems[0].Data);
        
        const columns = keys.map(key => {
            return new textColumn<Raven.Server.Documents.Indexes.Debugging.MapResultInLeaf>(this.gridController(), x => x.Data[key], key,  (80 / keys.length) + "%");
        });

        columns.push(new textColumn<Raven.Server.Documents.Indexes.Debugging.MapResultInLeaf>(this.gridController(), x => x.Source || '-', "Source Document", "20%"));
        return columns;
    }

    private fetcher(skip: number, take: number): JQueryPromise<pagedResult<Raven.Server.Documents.Indexes.Debugging.MapResultInLeaf>> {
        return $.Deferred<pagedResult<Raven.Server.Documents.Indexes.Debugging.MapResultInLeaf>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }

}

export = visualizerTreeExplorer;
