/// <reference path="../../../typings/tsd.d.ts" />
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");

class columnItem {

    visible = ko.observable<boolean>(false);

    editable = ko.observable<boolean>(false);

    virtualColumn = ko.observable<virtualColumn>();

    visibileInSelector = ko.pureComputed(() => {
        const column = this.virtualColumn();
        const isMetadata = column.header === "__metadata";
        const isActionColumn = column instanceof actionColumn;
        return !(column instanceof checkedColumn) && !isActionColumn && !isMetadata;
    });

    constructor(col: virtualColumn, editable: boolean) {
        this.virtualColumn(col);
        this.editable(editable);
    }
}

class columnsSelector<T> {

    private readonly grid: virtualGridController<T>;
    private readonly fetcher: (skip: number, take: number, columns: string[]) => JQueryPromise<pagedResult<T>>;
    private readonly defaultColumnsProvider: (containerWidth: number, results: pagedResult<T>) => virtualColumn[];
    private readonly availableColumnsProvider: (results: pagedResult<T>) => string[];

    constructor(grid: virtualGridController<T>, fetcher: (skip: number, take: number, columns: string[]) => JQueryPromise<pagedResult<T>>,
        defaultColumnsProvider: (containerWidth: number, results: pagedResult<T>) => virtualColumn[],
        availableColumnsProvider: (results: pagedResult<T>) => string[]) {
        this.grid = grid;
        this.fetcher = fetcher;
        this.defaultColumnsProvider = defaultColumnsProvider;
        this.availableColumnsProvider = availableColumnsProvider;
    }

    columnLayout = ko.observableArray<columnItem>();

    private customLayout = ko.observable<boolean>(false);

    initGrid() {
        //TODO: if we have custom columns applied init with custom
        this.grid.init((s, t) => this.onFetch(s, t), (w, r) => this.provideColumns(w, r));
    }

    applyColumns() {
        this.customLayout(true);
        this.grid.reset(true);
    }

    useDefaults() {
        this.reset();
        this.grid.reset(true);
    }

    private onFetch(skip: number, take: number): JQueryPromise<pagedResult<T>> {
        const fetchTask = $.Deferred<pagedResult<T>>();

        this.fetcher(skip, take, this.findColumnsToFetch())
            .done((result) => {
                const allColumns = this.availableColumnsProvider(result);
                this.registerKnownColumns(allColumns);
                fetchTask.resolve(result);
            })
            .fail((xhr: JQueryXHR) => fetchTask.reject(xhr));

        return fetchTask;
    }

    private registerKnownColumns(names: string[]) {
        for (let i = 0; i < names.length; i++) {
            const name = names[i];

            const existingColumn = this.columnLayout().find(x => x.virtualColumn().header === name);
            if (!existingColumn) {
                const virtColumn = new textColumn(name, name, "200px"); //TODO: use factory?
                this.columnLayout.push(new columnItem(virtColumn, false));
            }
        }
    }

    private provideColumns(containerWidth: number, results: pagedResult<T>): virtualColumn[] {
        if (this.customLayout()) {
            return this.columnLayout()
                .filter(x => x.visible())
                .map(x => x.virtualColumn());
        } else {
            const innerProviderColumns = this.defaultColumnsProvider(containerWidth, results);
            this.syncColumnLayout(innerProviderColumns);
            return innerProviderColumns;
        }
    }

    private syncColumnLayout(columns: virtualColumn[]) {
        _.forEachRight(columns, column => {
            const existingColumn = this.columnLayout().find(x => x.virtualColumn().header === column.header);
            if (existingColumn) {
                // put on top and set as visible, incoming virtualColumn overrides existing copy (in order to sync width, etc).

                existingColumn.visible(true);
                existingColumn.virtualColumn(column);

                this.columnLayout.remove(existingColumn);
                this.columnLayout.unshift(existingColumn);
            } else {
                // new column (probably non-standard)
                const newColumnItem = new columnItem(column, false);
                newColumnItem.visible(true);
                this.columnLayout.unshift(newColumnItem);
            }
        });
    }

    private findColumnsToFetch(): string[] { //TODO: is this class right location?
        if (this.customLayout()) {
            const toFetch = [] as string[];

            //TODO: introduce isSimpleBinding property on columnItem and filter using this

            this.columnLayout().forEach(item => {
                if (item.visible() && item.virtualColumn() instanceof textColumn) {
                    const text = item.virtualColumn() as textColumn<T>;
                    if (_.isString(text.valueAccessor)) {
                        toFetch.push(text.valueAccessor);
                    }
                }
            });

            return toFetch;
        } else {
            return undefined;
        }
    }

    reset() {
        this.customLayout(false);
        this.columnLayout.removeAll();
    }

    saveAsDefault(contextName: string) {
        //TODO:
    }


    loadDefaults(contextName: string) {
        //TODO:
    }
}

export = columnsSelector;
