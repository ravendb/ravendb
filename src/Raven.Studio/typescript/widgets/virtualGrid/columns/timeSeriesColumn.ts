/// <reference path="../../../../typings/tsd.d.ts"/>
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import virtualRow = require("widgets/virtualGrid/virtualRow");
import generalUtils = require("common/generalUtils");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import timeSeriesQueryResult = require("models/database/timeSeries/timeSeriesQueryResult");

/**
 * Virtual grid column that renders time series.
 */
class timeSeriesColumn<T> extends textColumn<T> {
    
    static readonly dateFormat = "YYYY-MM-DD";

    private readonly handler: (type: timeSeriesColumnEventType, document: T, path: string, event: JQueryEventObject) => void;

    tsPlotActionUniqueId = _.uniqueId("ts-plot-action-");
    tsPreviewActionUniqueId = _.uniqueId("ts-preview-action-");

    constructor(gridController: virtualGridController<T>, valueAccessor: ((obj: T) => any) | string, header: string, width: string, opts: timeSeriesColumnOpts<T> = {}) {
        super(gridController, valueAccessor, header, width, opts);
        
        if (typeof valueAccessor !== "string") {
            throw new Error("Only string based value accessor in supported");
        }
        
        this.handler = opts.handler;
    }

    canHandle(actionId: string) {
        const canHandlePlot = this.handler && this.tsPlotActionUniqueId === actionId;
        const canHandlePreview = this.handler && this.tsPreviewActionUniqueId === actionId;
        return canHandlePlot || canHandlePreview;
    }

    handle(row: virtualRow, event: JQueryEventObject, actionId: string) {
        const value = this.getCellValue(row.data as T);
        if (actionId === this.tsPlotActionUniqueId) {
            this.handler("plot", row.data as T, this.valueAccessor as string, event);
        } else if (actionId === this.tsPreviewActionUniqueId) {
            this.handler("preview", row.data as T, this.valueAccessor as string, event);
        } else {
            console.warn("Unsupported action type = " + actionId);
        }
    }

    renderCell(item: T, isSelected: boolean, isSorted: boolean): string {
        const cellValue = this.getCellValue(item);
        const preparedValue = this.prepareValue(item);
        if (!cellValue) {
            return super.renderCell(item, isSelected, isSorted);
        }
        
        const customPlotAction = `data-action="${this.tsPlotActionUniqueId}" `;
        const customPreviewAction = `data-action="${this.tsPreviewActionUniqueId}" `;

        const extraHtml = this.opts.title ? ` title="${generalUtils.escapeHtml(this.opts.title(item))}" ` : '';
        let extraCssClasses = this.opts.extraClass ? this.opts.extraClass(item) : '';

        if (isSorted) {
            extraCssClasses += ' sorted';
        }

        try {
            const model = new timeSeriesQueryResult(cellValue); 
            const [startDate, endDate] = model.getDateRange();
            
            const dateRange = startDate && endDate 
                ? generalUtils.formatUtcDateAsLocal(startDate, timeSeriesColumn.dateFormat) + " - " + generalUtils.formatUtcDateAsLocal(endDate, timeSeriesColumn.dateFormat)
                : "N/A";
                
            const tsInfo = `<i title="Time Series" class="icon-timeseries margin-right"></i>` 
                + `<div class="ts-group-property" data-label="Count">${model.getCount().toLocaleString()}</div>`
                + `<div class="ts-group-property" data-label="Date Range">${dateRange}</div>`
            ;
            
            const plotButton = this.handler 
                ? `<button title="Plot time series graph" class="btn btn-default btn-sm" ${customPlotAction}><i class="icon-stats"></i></button>`
                : "";
            
            const previewButton = this.handler 
                ? `<button title="Show time series values" class="btn btn-default btn-sm" ${customPreviewAction}><i class="icon-preview"></i></button>`
                : "";
            
            const separator = `<div class="flex-separator"></div>`;
            const valueContainer = `<div class="flex-horizontal">${tsInfo}${separator}${previewButton}&nbsp;${plotButton}</div>`;
            return `<div  ${extraHtml} class="cell text-cell flex-horizontal ${preparedValue.typeCssClass} ${extraCssClasses}" style="width: ${this.width}">${valueContainer}</div>`;
        } catch (error) {
            return `<div class="cell text-cell eval-error ${extraCssClasses}" style="width: ${this.width}">Error!</div>`;
        }
    }

    toDto(): virtualColumnDto {
        return {
            type: "timeSeries",
            header: this.header,
            width: this.width,
            serializedValue: this.valueAccessor.toString()
        };
    }
}

export = timeSeriesColumn;
