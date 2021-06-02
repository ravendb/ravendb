/// <reference path="../../../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import customColumn = require("widgets/virtualGrid/columns/customColumn");
import flagsColumn = require("widgets/virtualGrid/columns/flagsColumn");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import timeSeriesColumn = require("widgets/virtualGrid/columns/timeSeriesColumn");
import inlineTimeSeriesColumn = require("widgets/virtualGrid/columns/inlineTimeSeriesColumn");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import appUrl = require("common/appUrl");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import database = require("models/resources/database");
import document = require("models/database/documents/document");
import virtualGridUtils = require("widgets/virtualGrid/virtualGridUtils");
import app = require("durandal/app");
import showDataDialog = require("viewmodels/common/showDataDialog");
import generalUtils = require("common/generalUtils");


type columnOptionsDto = {
    extraClass?: (item: document) => string;
}

type documentBasedColumnsProviderOpts = {
    showRowSelectionCheckbox?: boolean;
    enableInlinePreview?: boolean;
    customInlinePreview?: (doc: document) => void;
    showSelectAllCheckbox?: boolean;
    createHyperlinks?: boolean;
    columnOptions?: columnOptionsDto;
    showFlags?: boolean; // revisions, counters, attachments
    detectTimeSeries?: boolean;
    timeSeriesActionHandler?: (type: timeSeriesColumnEventType, documentId: string, name: string, value: timeSeriesQueryResultDto, event: JQueryEventObject) => void;
}

class documentBasedColumnsProvider {
    
    static readonly rootedTimeSeriesResultMarker = "@@__TimeSeries__@@";

    private static readonly minColumnWidth = 150;

    showRowSelectionCheckbox: boolean;
    private readonly db: database;
    private readonly gridController: virtualGridController<document>;
    private readonly enableInlinePreview: boolean;
    private readonly createHyperlinks: boolean;
    private readonly showFlags: boolean;
    private readonly detectTimeSeries: boolean;
    private readonly showSelectAllCheckbox: boolean;
    private readonly columnOptions: columnOptionsDto;
    private readonly customInlinePreview: (doc: document) => void;
    private readonly collectionTracker: collectionsTracker;
    private readonly customColumnProvider: () => virtualColumn[];
    private readonly timeSeriesActionHandler: (type: timeSeriesColumnEventType, documentId: string, name: string, value: timeSeriesQueryResultDto, event: JQueryEventObject) => void;

    private static readonly externalIdRegex = /^\w+\/\w+/ig;

    constructor(db: database, gridController: virtualGridController<document>, opts: documentBasedColumnsProviderOpts) {
        this.showRowSelectionCheckbox = _.isBoolean(opts.showRowSelectionCheckbox) ? opts.showRowSelectionCheckbox : false;
        this.db = db;
        this.gridController = gridController;
        this.enableInlinePreview = _.isBoolean(opts.enableInlinePreview) ? opts.enableInlinePreview : false;
        this.showSelectAllCheckbox = _.isBoolean(opts.showSelectAllCheckbox) ? opts.showSelectAllCheckbox : false;
        this.createHyperlinks = _.isBoolean(opts.createHyperlinks) ? opts.createHyperlinks : true;
        this.columnOptions = opts.columnOptions;
        this.customInlinePreview = opts.customInlinePreview || documentBasedColumnsProvider.showPreview;
        this.collectionTracker = collectionsTracker.default;
        this.showFlags = !!opts.showFlags;
        this.detectTimeSeries = opts.detectTimeSeries || false;
        this.timeSeriesActionHandler = opts.timeSeriesActionHandler;
    }

    findColumns(viewportWidth: number, results: pagedResult<document>, prioritizedColumns?: string[]): virtualColumn[] {
        if (this.customColumnProvider) {
            try {
                const customColumns = this.customColumnProvider();
                
                if (customColumns) {
                    return customColumns;
                }
            } catch (e) {
                console.error(e);
                // fall through here  - we want to execute default action if restore failed
            }
        }
        
        let columnNames = this.findColumnNames(results, Math.floor(viewportWidth / documentBasedColumnsProvider.minColumnWidth), prioritizedColumns);
        const timeSeriesColumns = this.findTimeSeriesColumns(results);
        
        const hasTimeSeries = !!timeSeriesColumns.length;
        const includeInlineTimeSeriesColumn = timeSeriesColumns.length === 1 
            && timeSeriesColumns[0] === documentBasedColumnsProvider.rootedTimeSeriesResultMarker; 
        
        if (includeInlineTimeSeriesColumn) {
            // result contains inline time series result, replace Count and Results column with single TimeSeries column
            const keysToStrip: Array<keyof timeSeriesQueryResultDto> = ["Count", "Results"];
            columnNames = _.without(columnNames, ...keysToStrip);
            columnNames.push(documentBasedColumnsProvider.rootedTimeSeriesResultMarker);
        }
        
        // Insert the row selection checkbox column as necessary.
        const initialColumns: virtualColumn[] = [];

        if (this.showRowSelectionCheckbox || hasTimeSeries) {
            initialColumns.push(new checkedColumn(this.showSelectAllCheckbox));
        }

        if (this.enableInlinePreview) {
            const previewColumn = new actionColumn<document>(this.gridController, this.customInlinePreview, "Preview", `<i class="icon-preview"></i>`, "75px",
            {
                title: () => 'Show item preview'
            });
            initialColumns.push(previewColumn);
        }
        
        let flags: flagsColumn = null;
        
        if (this.showFlags) {
            flags = new flagsColumn(this.gridController);
            initialColumns.push(flags);
        }

        const tsOptions = {
            ...this.columnOptions,
            handler: this.timeSeriesActionHandler
        } as timeSeriesColumnOpts<any>;

        const initialColumnsWidth = _.sumBy(initialColumns, x => virtualGridUtils.widthToPixels(x));
        const rightScrollWidth = 5;
        const remainingSpaceForOtherColumns = viewportWidth - initialColumnsWidth - rightScrollWidth;
        const columnWidth = Math.floor(remainingSpaceForOtherColumns / columnNames.length) + "px";

        const finalColumns = initialColumns.concat(columnNames.map(p => {
            if (includeInlineTimeSeriesColumn && p === documentBasedColumnsProvider.rootedTimeSeriesResultMarker) {
                return new inlineTimeSeriesColumn(this.gridController, columnWidth, tsOptions);
            }
            
            if (_.includes(timeSeriesColumns, p)) {
                return new timeSeriesColumn(this.gridController, p, generalUtils.escapeHtml(p), columnWidth, tsOptions);
            }
            
            if (this.createHyperlinks) {
                if (p === "__metadata") {
                    return new hyperlinkColumn(this.gridController, document.createDocumentIdProvider(), x => appUrl.forEditDoc(x.getId(), this.db, x.__metadata.collection), "Id", columnWidth, this.columnOptions);
                }

                return new hyperlinkColumn(this.gridController, p, _.partial(this.findLink, _, p).bind(this), generalUtils.escapeHtml(p), columnWidth, this.columnOptions);
            } else {
                if (p === "__metadata") {
                    return new textColumn(this.gridController, document.createDocumentIdProvider(), "Id", columnWidth, this.columnOptions);
                }
                return new textColumn(this.gridController, p, generalUtils.escapeHtml(p), columnWidth, this.columnOptions);
            }
        }));
        
        if (flags) {
            // move this column to the end
            _.pull(finalColumns, flags);
            finalColumns.push(flags);
        }
        
        return finalColumns;
    }

    reviver(source: virtualColumnDto): virtualColumn {
        if (source.type === "hyperlink" && source.serializedValue === document.customColumnName) {
            return new hyperlinkColumn(this.gridController,
                document.createDocumentIdProvider(), 
                    x => appUrl.forEditDoc(x.getId(), this.db, x.__metadata.collection),
                source.header, source.width, this.columnOptions);
        }
        
        switch (source.type) {
            case "flags":
                return new flagsColumn(this.gridController);
            case "checkbox":
                return new checkedColumn(this.showSelectAllCheckbox);
            case "text":
                return new textColumn(this.gridController, source.serializedValue, source.header, source.width, this.columnOptions);
            case "hyperlink":
                return new hyperlinkColumn(this.gridController, source.serializedValue, _.partial(this.findLink, _, source.serializedValue).bind(this), source.header, source.width, this.columnOptions);
            case "custom":
                return new customColumn(this.gridController, source.serializedValue, source.header, source.width);
            default:
                throw new Error("Unhandled column type: " + source);
        }
    }
    
    static showPreview(doc: document) {
        const docDto = doc.toDto(true);
        
        const text = JSON.stringify(docDto, null, 4);
        const title = doc.getId() ? "Document: " + doc.getId() : "Document preview";
        app.showBootstrapDialog(new showDataDialog(title, text, "javascript"));
    }

    static extractUniquePropertyNames(results: pagedResult<document>) {
        const uniquePropertyNames = new Set<string>();

        results.items
            .map(i => _.keys(i).forEach(key => uniquePropertyNames.add(key)));

        if (!_.every(results.items, (x: document) => x.__metadata && x.getId())) {
            uniquePropertyNames.delete("__metadata");
        }

        return Array.from(uniquePropertyNames);
    }

    private findTimeSeriesColumns(results: pagedResult<document>): Array<string> {
        if (!this.detectTimeSeries) {
            return [];
        }

        if (results.items.length === 0) {
            return [];
        }
        
        const timeSeriesFields = results.additionalResultInfo ? results.additionalResultInfo.TimeSeriesFields : undefined;
        if (timeSeriesFields != null) {
            if (timeSeriesFields.length === 0) {
                // special case timeSeriesFields is empty array, it means result contains time series data at root level (w/o alias)
                return [documentBasedColumnsProvider.rootedTimeSeriesResultMarker];
            } else {
                return timeSeriesFields;
            }
        } else {
            return [];
        }
    }
    
    private findColumnNames(results: pagedResult<document>, limit: number, prioritizedColumns?: string[]): string[] {
        const columnNames = documentBasedColumnsProvider.extractUniquePropertyNames(results);

        prioritizedColumns = prioritizedColumns || ["__metadata", "Name"];

        prioritizedColumns
            .reverse()
            .forEach(c => {
                const columnIndex = columnNames.indexOf(c);
                if (columnIndex >= 0) {
                    columnNames.splice(columnIndex, 1);
                    columnNames.unshift(c);
                }
            });

        if (columnNames.length > limit) {
            columnNames.length = limit;
        }

        return columnNames;
    }

    private findLink(item: document, property: string): string {
        if (property in item) {
            const value = (item as any)[property];

            //TODO: support url's in data as well
            if (_.isString(value) && value.match(documentBasedColumnsProvider.externalIdRegex)) {
                const extractedCollectionName = value.split("/")[0].toLowerCase();
                const matchedCollection = this.collectionTracker.getCollectionNames().find(collection => extractedCollectionName.startsWith(collection.toLowerCase()));
                return matchedCollection ? appUrl.forEditDoc(value, this.db, matchedCollection) : null;
            }
        }
        return null;
    }
}

export = documentBasedColumnsProvider;
