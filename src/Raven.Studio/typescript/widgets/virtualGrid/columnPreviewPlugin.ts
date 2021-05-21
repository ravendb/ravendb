/// <reference path="../../../typings/tsd.d.ts"/>
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGrid = require("widgets/virtualGrid/virtualGrid");
import copyToClipboard = require("common/copyToClipboard");
import generalUtils = require("common/generalUtils");

class columnPreviewPlugin<T> {

    private grid: virtualGrid<T>;
    private previewTimeoutHandle: number;
    private enterTooltipTimeoutHandle: number;
    private previewVisible = false;
    private $tooltip: JQuery;
    private currentValue: any;

    static localDateFormat = "YYYY-MM-DD HH:mm:ss.SSS";

    private static readonly delay = 500;
    private static readonly enterTooltipDelay = 100;
    
    private static defaultMarkupProvider(value: any) {
        const copySyntax = '<button class="btn btn-default btn-sm copy"><i class="icon-copy"></i><span>Copy to clipboard</span></button>';
        
        if (moment.isMoment(value)) { // value instanceof moment isn't reliable 
            const dateAsMoment = value as moment.Moment;
            const fullDuration = generalUtils.formatDurationByDate(moment.utc(dateAsMoment), true);
            
            const isUtc = dateAsMoment.isUtc();
            const dateFormatted = isUtc ? dateAsMoment.format() : dateAsMoment.format(columnPreviewPlugin.localDateFormat);
            
            return `<div class="dataContainer">
                        <div>
                            <div class="dateLabel">${isUtc ? "UTC" : "Local"}: </div>
                            <div class="dateValue">${dateFormatted}</div>
                        </div>
                        <div>
                            <div class="dataLabel">Relative: </div>
                            <div class="dataValue">${fullDuration}</div>
                        </div>
                </div>` + copySyntax;
        } else {
            return '<pre><code class="white-space-pre">' + value + '</code></pre>' + copySyntax;
        }
    }

    install(containerSelector: string, tooltipSelector: string, 
            previewContextProvider: (item: T, column: virtualColumn, event: JQueryEventObject, onValueProvided: (context: any, valueToCopy?: any) => void) => void) {
        
        const $grid = $(containerSelector + " .virtual-grid");
        const grid = ko.dataFor($grid[0]) as virtualGrid<T>;
        if (!grid || !(grid instanceof virtualGrid)) {
            throw new Error("Unable to find virtualGrid");
        }

        this.$tooltip = $(tooltipSelector);

        this.grid = grid;
        
        const markupProvider = columnPreviewPlugin.defaultMarkupProvider;
        
        this.$tooltip.on("click", ".copy", () => {
            copyToClipboard.copy(this.currentValue, "Item has been copied to clipboard", document.querySelector(containerSelector));
            
            $(".copy", this.$tooltip).addClass("btn-success");
            $(".copy span", this.$tooltip)
                .html("Copied!")
        });

        $(containerSelector).on("mouseenter", ".cell", e => {
            const [element, column] = this.findItemAndColumn(e);
            this.previewTimeoutHandle = setTimeout(() => {
                if (document.body.contains(e.target)) {
                    this.previewVisible = true;

                    previewContextProvider(element, column, e, (value, valueToCopy) => {
                        const markup = markupProvider(value);
                        this.show(markup, e); 
                        this.currentValue = _.isUndefined(valueToCopy) ? value : valueToCopy;
                    });    
                }
            }, columnPreviewPlugin.delay);
        });

        $(containerSelector).on("mouseleave", ".cell", () => {
            clearTimeout(this.previewTimeoutHandle);
            this.previewTimeoutHandle = undefined;

            if (this.previewVisible) {
                // if preview is visible and mouse is out - give 100 ms to enter to tooltip, before hide
                this.enterTooltipTimeoutHandle = setTimeout(() => {
                    this.hide();
                    this.previewVisible = false;
                    this.currentValue = undefined;
                }, columnPreviewPlugin.enterTooltipDelay);
            }
        });

        $(tooltipSelector).on("mouseenter", () => {
            if (this.enterTooltipTimeoutHandle) {
                clearTimeout(this.enterTooltipTimeoutHandle);
                this.enterTooltipTimeoutHandle = undefined;
            }
        });

        $(tooltipSelector).on("mouseleave", () => {
            if (this.previewVisible) {
                this.hide();
                this.previewVisible = false;
            }
        });
    }

    show(markup: string, e: JQueryEventObject) {
        const $parent = this.$tooltip.parent().offsetParent();
        const parentOffset = $parent.offset();
        const $cell = $(e.target).closest(".cell");
        const cellOffset = $cell.offset();

        const parentWidth = $parent.outerWidth();
        const parentHeight = $parent.outerHeight();

        const left = cellOffset.left - parentOffset.left;
        
        // position in top left corner to measure
        this.$tooltip
            .css('left', '0px')
            .css('right', '')
            .css('top', '0px')
            .css('bottom', '');

        this.$tooltip.html(markup);
        
        const computedWidth = this.$tooltip.outerWidth();
        const computedHeight = this.$tooltip.outerHeight();
               
        if (left + computedWidth < parentWidth) {
            this.$tooltip
                .css('left', left + 'px')
                .css('right', '');
        } else {
            const right = parentWidth - left - $cell.outerWidth();
            this.$tooltip
                .css('left', '')
                .css('right', right + 'px');
        }

        const top = cellOffset.top - parentOffset.top + $cell.outerHeight();
        if (top + computedHeight < parentHeight) {
            this.$tooltip
                .css('top', top + 'px')
                .css('bottom', '');
        } else {
            const bottom = parentHeight - top + $cell.outerHeight();
            this.$tooltip
                .css('top', '')
                .css('bottom', bottom + 'px');
        }

        this.$tooltip
            .css('opacity', 1)
            .show();
    }

    hide() {
        this.$tooltip
            .css('opacity', 0)
            .hide();
        this.$tooltip.html("");
    }
    
    private findItemAndColumn(e: JQueryEventObject): [T, virtualColumn] {
        const row = this.grid.findRowForCell(e.target);
        const column = this.grid.findColumnForCell(e.target);
        return [row.data as T, column];
    }
}

export = columnPreviewPlugin;
