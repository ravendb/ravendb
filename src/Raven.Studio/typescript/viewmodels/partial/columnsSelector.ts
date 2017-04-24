/// <reference path="../../../typings/tsd.d.ts" />
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import customColumn = require("widgets/virtualGrid/columns/customColumn");

class columnItem {

    visible = ko.observable<boolean>(false);

    editable = ko.observable<boolean>(false);

    virtualColumn = ko.observable<virtualColumn>();

    visibleInSelector = ko.pureComputed(() => {
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

class customColumnForm {
    formVisible = ko.observable<boolean>(false);

    header = ko.observable<string>();
    expression = ko.observable<string>();

    parseError: KnockoutComputed<string>;

    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        header: this.header,
        expression: this.expression
    });

    private editedItem = ko.observable<columnItem>();

    constructor() {
        this.initValidation();

        this.parseError = ko.pureComputed(() => {
            const exp = this.expression();
            if (exp) {
                return this.tryParse(exp);
            }
            return null;
        });
    }

    private tryParse(val: string): string {
        try {
            // ReSharper disable once WrongExpressionStatement
            new Function('return (' + val + ")");
            return null;
        } catch (e) {
            return (e as Error).message;
        }
    }

    private initValidation() {
        this.header.extend({
            required: true
        });

        this.expression.extend({
            required: true
        });

        this.expression.extend({
            validation: [{
                validator: (val: string) => !this.tryParse(val),
                message: "Unable to parse binding expression."
            }]
        });
    }

    asColumnItem(): columnItem {
        const editedItem = this.editedItem();
        if (editedItem) {
            editedItem.virtualColumn().header = this.header();
            (editedItem.virtualColumn() as customColumn<any>).setJsCode(this.expression());
            return editedItem;
        } else {
            const newColumn = new customColumn(this.expression(), this.header(), columnsSelector.defaultWidth);
            const newColumnItem = new columnItem(newColumn, true);
            newColumnItem.visible(true);
            return newColumnItem;
        }
    }

    reset() {
        this.header("");
        this.expression("");
        this.editedItem(null);
        this.formVisible(false);
    }

    edit(columnToEdit: columnItem) {
        this.formVisible(true);
        this.editedItem(columnToEdit);

        const column = columnToEdit.virtualColumn() as customColumn<any>;

        this.header(column.header);
        this.expression(column.jsCode);
    }
}

class columnsSelector<T> {

    static readonly defaultWidth = "200px";

    private grid: virtualGridController<T>;
    private fetcher: (skip: number, take: number, previewColumns: string[], fullColumns: string[]) => JQueryPromise<pagedResult<T>>;
    private defaultColumnsProvider: (containerWidth: number, results: pagedResult<T>) => virtualColumn[];
    private availableColumnsProvider: (results: pagedResult<T>) => string[];
    private customLayout = ko.observable<boolean>(false);

    customColumnForm = new customColumnForm();
    columnLayout = ko.observableArray<columnItem>();
    selectionState: KnockoutComputed<checkbox>;

    constructor() {
        this.initObservables();
    }

    private initObservables() {
        this.selectionState = ko.pureComputed<checkbox>(() => {
            const allVisibleColumns = this.columnLayout().filter(x => x.visibleInSelector());
            const checkedCount = allVisibleColumns.filter(x => x.visible()).length;

            if (allVisibleColumns.length && checkedCount === allVisibleColumns.length)
                return checkbox.Checked;

            return  checkedCount > 0 ? checkbox.SomeChecked : checkbox.UnChecked;
        });
    }

    init(grid: virtualGridController<T>, fetcher: (skip: number, take: number, previewColumns: string[], fullColumns: string[]) => JQueryPromise<pagedResult<T>>,
        defaultColumnsProvider: (containerWidth: number, results: pagedResult<T>) => virtualColumn[],
        availableColumnsProvider: (results: pagedResult<T>) => string[]) {
        this.grid = grid;
        this.fetcher = fetcher;
        this.defaultColumnsProvider = defaultColumnsProvider;
        this.availableColumnsProvider = availableColumnsProvider;

        //TODO: restore custom colums (if any) - optionally introduce option to disallow customization - make sure it works properly with required columns

        this.grid.init((s, t) => this.onFetch(s, t), (w, r) => this.provideColumns(w, r));
    }

    compositionComplete() {
        this.registerSortable();
        $("#custom_column_binding").tooltip({
            trigger: 'focus'
        });
    }

    private registerSortable() {
        const list = $(".columns-list-container .column-list")[0];

        // ReSharper disable once WrongExpressionStatement
        const sort = new Sortable(list,
        {
            handle: ".column-rearrange",
            onEnd: (event: { oldIndex: number, newIndex: number }) => {
                const layout = this.columnLayout();
                layout.splice(event.newIndex, 0, layout.splice(event.oldIndex, 1)[0]);
                this.columnLayout(layout);
            }
        });

        ko.utils.domNodeDisposal.addDisposeCallback(list, () => {
            sort.destroy();
        });
    }

    toggleSelectAll(): void {
        const allVisibleColumns = this.columnLayout().filter(x => x.visibleInSelector());

        const selectedCount = allVisibleColumns.filter(x => x.visible()).length;

        if (selectedCount > 0) {
            allVisibleColumns.forEach(x => x.visible(false));
        } else {
            allVisibleColumns.forEach(x => x.visible(true));
        }
    }

    applyColumns() {
        this.customLayout(true);
        this.grid.reset(true);
    }

    addCustomColumn() {
        if (this.customColumnForm.validationGroup.isValid()) {
            const item = this.customColumnForm.asColumnItem();

            if (!_.includes(this.columnLayout(), item)) {
                this.columnLayout.push(item);
            }

            // sync header name
            item.virtualColumn.valueHasMutated();

            this.customColumnForm.reset();
            this.applyColumns();
        }
    }

    cancelEditMode() {
        this.customColumnForm.reset();
    }

    showAddCustomColumnForm() {
        if (this.customColumnForm.formVisible()) {
            // if form is open assume user wants to submit form
            this.addCustomColumn();
        } else {
            this.customColumnForm.reset();
            this.customColumnForm.formVisible(true);            
        }
    }

    removeColumn(col: columnItem) {
        this.columnLayout.remove(col);
    }

    editColumn(columnToEdit: columnItem) {
        this.customColumnForm.edit(columnToEdit);
    }

    useDefaults() {
        this.reset();
        this.grid.reset(true);
    }

    reset() {
        this.customLayout(false);
        this.columnLayout.removeAll();
    }

    private onFetch(skip: number, take: number): JQueryPromise<pagedResult<T>> {
        const fetchTask = $.Deferred<pagedResult<T>>();

        const cols = this.findColumnsToFetch();

        this.fetcher(skip, take, cols.preview, cols.full)
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
                const virtColumn = new textColumn(name, name, columnsSelector.defaultWidth);
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

    private findColumnsToFetch(): { full: string[], preview: string[] } {
        if (this.customLayout()) {
            const fullColumns = [] as string[];
            const previewColumns = [] as string[];

            this.columnLayout().forEach(item => {
                if (item.visible()) {
                    if (item.virtualColumn() instanceof textColumn) {
                        const text = item.virtualColumn() as textColumn<T>;
                        if (_.isString(text.valueAccessor)) {
                            previewColumns.push(text.valueAccessor);
                        }
                    }

                    if (item.virtualColumn() instanceof customColumn) {
                        const customCol = item.virtualColumn() as customColumn<T>;
                        fullColumns.push(...customCol.tryGuessRequiredProperties());
                    }
                }
            });

            const full = _.uniq(fullColumns);
            const preview = _.without(_.uniq(previewColumns), ...full);

            return {
                full: full,
                preview: preview
            };
        } else {
            return {
                full: undefined,
                preview: undefined
            };
        }
    }

    saveAsDefault(contextName: string) {
        //TODO:
    }


    loadDefaults(contextName: string) {
        //TODO:
    }
}

export = columnsSelector;
