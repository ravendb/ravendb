/// <reference path="../../../typings/tsd.d.ts"/>
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGrid = require("widgets/virtualGrid/virtualGrid");
import copyToClipboard = require("common/copyToClipboard");
import generalUtils = require("common/generalUtils");
import moment = require("moment");


class copyFeature implements columnPreviewFeature {
    install($tooltip: JQuery, valueProvider: () => any, elementProvider: () => any, containerSelector: string) {
        $tooltip.on("click", ".copy", () => {
            copyToClipboard.copy(valueProvider(), "Item has been copied to clipboard", document.querySelector(containerSelector));

            $(".copy", $tooltip).addClass("btn-success");
            $(".copy span", $tooltip)
                .html("Copied!")
        });
    }
    
    syntax() {
        return '<button class="btn btn-default btn-sm copy"><i class="icon-copy"></i><span>Copy to clipboard</span></button>';
    }
}

class columnPreviewPlugin<T extends object> {

    private grid: virtualGrid<T>;
    private previewTimeoutHandle: ReturnType<typeof setTimeout>;
    private enterTooltipTimeoutHandle: ReturnType<typeof setTimeout>;
    private exitTooltipTimeoutHandle: ReturnType<typeof setTimeout>;
    private previewVisible = false;
    private $tooltip: JQuery;
    private currentValue: any;
    private currentElement: any;
    
    private features: columnPreviewFeature[] = [new copyFeature()];
    
    static localDateFormat = "YYYY-MM-DD HH:mm:ss.SSS";

    private static readonly delay = 500;
    private static readonly enterTooltipDelay = 300;
    private static readonly exitTooltipDelay = 200;
    
    constructor() {
        _.bindAll(this, "defaultMarkupProvider");
    }
    
    private defaultMarkupProvider(value: any, column: virtualColumn, element: any, wrapValue: boolean = true) {
        const featuresSyntax = this.features.map(f => f.syntax(column, value, element)).join("");
        
        if (moment.isMoment(value)) { // value instanceof moment isn't reliable 
            const dateAsMoment = value as moment.Moment;
            const fullDuration = generalUtils.formatDurationByDate(moment.utc(dateAsMoment), true);
            
            const isUtc = dateAsMoment.isUtc();
            const dateFormatted = isUtc ? dateAsMoment.format() : dateAsMoment.format(columnPreviewPlugin.localDateFormat);
            
            return `<div class="data-container">
                        <div>
                            <div class="data-label">${isUtc ? "UTC" : "Local"}: </div>
                            <div class="data-value">${dateFormatted}</div>
                        </div>
                        <div>
                            <div class="data-label">Relative: </div>
                            <div class="data-value">${fullDuration}</div>
                        </div>
                    </div>` + featuresSyntax;
        } else {
            return wrapValue ? `<pre><code class="white-space-pre">${value}</code></pre>${featuresSyntax}` :
                               `${value}${featuresSyntax}`;
        }
    }

    install(containerSelector: string, tooltipSelector: string,
            previewContextProvider: (item: T, column: virtualColumn, event: JQuery.TriggeredEvent,
                                     onValueProvided: (value: any, valueToCopy?: any, wrapValue?: boolean) => void) => void, opts?: {
            additionalFeatures?: columnPreviewFeature[];
        }) {
        const $grid = $(containerSelector + " .virtual-grid");
        const grid = ko.dataFor($grid[0]) as virtualGrid<T>;
        if (!grid || !(grid instanceof virtualGrid)) {
            throw new Error("Unable to find virtualGrid");
        }

        this.$tooltip = $(tooltipSelector);

        this.grid = grid;
        
        if (opts?.additionalFeatures?.length > 0) {
            this.features.push(...opts.additionalFeatures);
        }
        
        const markupProvider = this.defaultMarkupProvider;

        for (const feature of this.features) {
            feature.install(this.$tooltip, () => this.currentValue, () => this.currentElement, containerSelector);
        }

        $(containerSelector).on("mouseenter", ".cell", e => {
            const [element, column] = this.findItemAndColumn(e);
            this.previewTimeoutHandle = setTimeout(() => {
                if (document.body.contains(e.target)) {
                    this.previewVisible = true;

                    previewContextProvider(element, column, e, (value, valueToCopy, wrapValue) => {
                        const markup = markupProvider(value, column, element, wrapValue);
                        this.show(markup, e);
                        this.currentValue = _.isUndefined(valueToCopy) ? value : valueToCopy;
                        this.currentElement = element;
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
            if (this.exitTooltipTimeoutHandle) {
                clearTimeout(this.exitTooltipTimeoutHandle);
                this.exitTooltipTimeoutHandle = undefined;
            }
        });

        $(tooltipSelector).on("mouseleave", () => {
            if (this.previewVisible) {
                this.exitTooltipTimeoutHandle = setTimeout(() => {
                this.hide();
                this.previewVisible = false;
                }, columnPreviewPlugin.exitTooltipDelay);
            }
        });
    }

    show(markup: string, e: JQuery.TriggeredEvent) {
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
                .css('right', '')
                .css("max-width", '');
        } else {
            const outerWidth = $cell.outerWidth();
            const right = parentWidth - left - outerWidth;
            
            this.$tooltip
                .css("max-width", (left + outerWidth) + "px")
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
    
    private findItemAndColumn(e: JQuery.MouseEnterEvent): [T, virtualColumn] {
        const row = this.grid.findRowForCell(e.target);
        const column = this.grid.findColumnForCell(e.target);
        return [row.data as T, column];
    }
}

export = columnPreviewPlugin;
