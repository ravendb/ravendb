/// <reference path="../../../../typings/tsd.d.ts"/>
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import virtualRow = require("widgets/virtualGrid/virtualRow");
import generalUtils = require("common/generalUtils");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import timeSeriesQueryResult = require("models/database/timeSeries/timeSeriesQueryResult");
import document = require("models/database/documents/document");

/**
 * Virtual grid column that renders time series.
 */
class timeSeriesColumn<T extends document> extends textColumn<T> {

    private readonly handler: (type: timeSeriesColumnEventType, documentId: string, name: string, value: timeSeriesQueryResultDto, event: JQueryEventObject) => void;

    tsPlotActionUniqueId = _.uniqueId("ts-plot-action-");
    tsPreviewActionUniqueId = _.uniqueId("ts-preview-action-");

    constructor(gridController: virtualGridController<T>, valueAccessor: string, header: string, width: string, opts: timeSeriesColumnOpts<T> = {}) {
        super(gridController, valueAccessor, header, width, opts);
        
        this.handler = opts.handler;
    }

    canHandle(actionId: string) {
        const canHandlePlot = this.handler && this.tsPlotActionUniqueId === actionId;
        const canHandlePreview = this.handler && this.tsPreviewActionUniqueId === actionId;
        return canHandlePlot || canHandlePreview;
    }
    
    getName() {
        return this.valueAccessor as string;
    }
    
    private isInputDisabled(element: Element): boolean {
        const closestButton = element.closest("button") as HTMLButtonElement;
        if (closestButton && closestButton.disabled) {
            // button is disabled 
            return true;
        }
        
        const closestLink = element.closest("a") as HTMLAnchorElement;
        if (closestLink && closestLink.classList.contains("disabled")) {
            return true;
        }
        
        return false;
    }

    handle(row: virtualRow, event: JQueryEventObject, actionId: string) {
       if (this.isInputDisabled(event.target)) {
           return;
       }
        
        const documentId = (row.data as T).getId();
        const name = this.getName();
        const value = this.getCellValue(row.data as T);
        
        
        if (actionId === this.tsPlotActionUniqueId) {
            this.handler("plot", documentId, name, value, event);
        } else if (actionId === this.tsPreviewActionUniqueId) {
            this.handler("preview", documentId, name, value, event);
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

        const titleHtml = this.opts.title ? ` title="${generalUtils.escapeHtml(this.opts.title(item))}" ` : '';
        let extraCssClasses = this.opts.extraClass ? this.opts.extraClass(item) : '';

        if (isSorted) {
            extraCssClasses += ' sorted';
        }

        try {
            const model = new timeSeriesQueryResult(cellValue); 
            const [startDate, endDate] = model.getDateRange();
            
            const resultType = model.detectResultType();
            
            const dateRange = startDate && endDate
                ? generalUtils.formatUtcDateAsLocal(startDate, generalUtils.basicDateFormat) + " - " + generalUtils.formatUtcDateAsLocal(endDate, generalUtils.basicDateFormat)
                : "N/A";
                
            const dateRangeTooltip = startDate && endDate
                ? generalUtils.formatUtcDateAsLocal(startDate, generalUtils.dateFormat) + " - " + generalUtils.formatUtcDateAsLocal(endDate, generalUtils.dateFormat)
                : "N/A";

            const tsInfo = `<i title="Time Series" class="icon-timeseries margin-right"></i>`
                + `<div class="ts-group-property" data-label="Points">${model.getCount().toLocaleString()}</div>`
                + (resultType === "grouped" ? `<div class="ts-group-property" data-label="Buckets">${model.getBucketCount().toLocaleString()}</div>` : "")
                + `<div class="ts-group-property date-range" data-label="Date Range" title="${dateRangeTooltip}">${dateRange}</div>`
            ;
            
            const plotButtonExtra = model.getCount() > 0 ? `` : ` disabled="disabled" `;
            const plotButton = this.handler 
                ? `<button title="Plot time series graph" ${plotButtonExtra} class="btn btn-default btn-sm" ${customPlotAction}><i class="icon-graph"></i></button>`
                : "";

            const previewButtonExtra = model.getCount() > 0 ? `` : ` disabled="disabled" `;
            const previewButton = this.handler 
                ? `<button title="Show time series values" ${previewButtonExtra} class="btn btn-default btn-sm" ${customPreviewAction}><i class="icon-table"></i></button>`
                : "";
            
            const separator = `<div class="flex-separator"></div>`;
            const valueContainer = `<div class="flex-horizontal">${tsInfo}${separator}${previewButton}&nbsp;${plotButton}</div>`;
            return `<div  ${titleHtml} class="cell text-cell flex-horizontal ${preparedValue.typeCssClass} ${extraCssClasses}" style="width: ${this.width}">${valueContainer}</div>`;
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
