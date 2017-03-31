/// <reference path="../../../typings/tsd.d.ts"/>
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGrid = require("widgets/virtualGrid/virtualGrid");

class columnPreviewPlugin<T> {

    private grid: virtualGrid<T>;
    private previewTimeoutHandle: number;
    private enterTooltipTimeoutHandle: number;
    private previewVisible = false;
    private $tooltip: JQuery;

    private static readonly delay = 500;
    private static readonly enterTooltipDelay = 100;
    private static readonly maxPreviewWindowSize = {
        // make sure it is in sync with virtual-grid.less: .json-preview pre style
        width: 500,
        height: 300
    }

    install(selector: string, tooltipSelector: string, previewContextProvider: (item: T, column: virtualColumn, event: JQueryEventObject, onValueProvided: (context: any) => void) => void) {
        const $grid = $(selector + " .virtual-grid");
        const grid = ko.dataFor($grid[0]) as virtualGrid<T>;
        if (!grid || !(grid instanceof virtualGrid)) {
            throw new Error("Unable to find virtualGrid");
        }

        this.$tooltip = $(tooltipSelector);

        this.grid = grid;

        $(selector).on("mouseenter", ".cell", e => {
            const [element, column] = this.findItemAndColumn(e);
            this.previewTimeoutHandle = setTimeout(() => {
                if (document.contains(e.target)) {
                    this.previewVisible = true;

                    previewContextProvider(element, column, e, markup => this.show(markup, e));    
                }
            }, columnPreviewPlugin.delay);
        });

        $(selector).on("mouseleave", ".cell", e => {
            clearTimeout(this.previewTimeoutHandle);
            this.previewTimeoutHandle = undefined;

            if (this.previewVisible) {
                // if preview is visible and mouse is out - give 100 ms to enter to tooltip, before hide
                this.enterTooltipTimeoutHandle = setTimeout(() => {
                    this.hide();
                    this.previewVisible = false;
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

        //TODO: add copy value button ?

        const parentWidth = $parent.outerWidth();
        const parentHeight = $parent.outerHeight(); //TODO: i think we sould use virtual grid height here!

        const left = cellOffset.left - parentOffset.left;
        if (left + columnPreviewPlugin.maxPreviewWindowSize.width < parentWidth) {
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
        if (top + columnPreviewPlugin.maxPreviewWindowSize.height < parentHeight) {
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

        $("code", this.$tooltip).html(markup);
    }

    hide() {
        this.$tooltip
            .css('opacity', 0)
            .hide();
        $("code", this.$tooltip).html("");
    }
    
    private findItemAndColumn(e: JQueryEventObject): [T, virtualColumn] {
        const row = this.grid.findRowForCell(e.target);
        const column = this.grid.findColumnForCell(e.target);
        return [row.data as T, column];
    }
}

export = columnPreviewPlugin;
