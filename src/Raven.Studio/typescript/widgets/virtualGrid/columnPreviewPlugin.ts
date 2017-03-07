/// <reference path="../../../typings/tsd.d.ts"/>
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGrid = require("widgets/virtualGrid/virtualGrid");

class columnPreviewPlugin<T> {

    private grid: virtualGrid<T>;
    private previewTimeoutHandle: number;
    private previewVisible = false;
    private $tooltip: JQuery;

    private static readonly delay = 500;

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
                this.previewVisible = true;

                previewContextProvider(element, column, e, markup => this.show(markup, e));

            }, columnPreviewPlugin.delay);
        });

        $(selector).on("mouseleave", ".cell", e => {
            clearTimeout(this.previewTimeoutHandle);
            this.previewTimeoutHandle = undefined;

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

        //TODO: fix positioning 

        this.$tooltip
            .css('opacity', 1)
            .css('left', (cellOffset.left - parentOffset.left) + 'px')
            .css('top', (cellOffset.top - parentOffset.top + $cell.outerHeight()) + 'px');
        $("code", this.$tooltip).html(markup);
    }

    hide() {
        this.$tooltip.css('opacity', 0);
        $("code", this.$tooltip).html("");
    }
    
    private findItemAndColumn(e: JQueryEventObject): [T, virtualColumn] {
        const row = this.grid.findRowForCell(e.target);
        const column = this.grid.findColumnForCell(e.target);
        return [row.data as T, column];
    }
}

export = columnPreviewPlugin;
