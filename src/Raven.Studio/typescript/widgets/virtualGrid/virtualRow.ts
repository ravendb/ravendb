/// <reference path="../../../typings/tsd.d.ts"/>
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");

/**
 * A virtual row. Contains an element displayed as a row in the grid. Gets recycled as the grid scrolls in order to create and manage fewer elements.
 */
class virtualRow {
    private _item: Object | null = null; // The last item populated into this virtual row.
    private sortColumnIndex: number = -1;
    private isItemSelected = false;
    readonly element: JQuery;
    private _top = -9999;
    private _index = -1;
    private _even: boolean | null = null;

    private _height: number;
    private readonly _disableStripes: boolean;
    
    constructor(height: number, disableStripes: boolean = false) {
        this._height = height;
        this._disableStripes = disableStripes;
        this.element = $(`<div class="virtual-row" style="height: ${this._height}px; top: ${this.top}px"></div>`);
    }

    get top(): number {
        return this._top;
    }

    get data(): Object {
        return this._item;
    }

    /**
     * Gets the index of the row this virtual row is displaying.
     */
    get index(): number {
        return this._index;
    }

    get hasData(): boolean {
        return !!this._item;
    }

    isOffscreen(scrollTop: number, scrollBottom: number) {
        const top = this.top;
        const bottom = top + this._height;
        return top > scrollBottom || bottom < scrollTop;
    }

    dataLoadError() {
        this.element.text(`Unable to load data`);
    }

    populate(item: Object | null, rowIndex: number, isSelected: boolean, columns: virtualColumn[], sortColumnIndex: number, customRowClasses: string[] = []) {
        // Optimization: don't regenerate this row HTML if nothing's changed since last render.
        const alreadyDisplayingData = !!item && this._item === item && this._index === rowIndex && this.isItemSelected === isSelected && this.sortColumnIndex === sortColumnIndex;
        if (!alreadyDisplayingData) {
            this._item = item;
            this._index = rowIndex;

            // If we have data, fill up this row content.
            if (item) {
                const html = this.createCellsHtml(item, columns, isSelected, sortColumnIndex);
                this.element.html(html);
            } else {
                this.element.text("");
            }

            let hasChangedSelectedStatus = this.isItemSelected !== isSelected;
            
            if (customRowClasses && customRowClasses.length) {
                this.element.attr("class", "virtual-row " + customRowClasses.join(" "));
                
                // since we force unselected state above - mark selection state to be updated if needed
                if (isSelected) {
                    hasChangedSelectedStatus = true;
                }
            }

            // Update the selected status. Displays as a different row color.
            if (hasChangedSelectedStatus) {
                this.element.toggleClass("selected", isSelected);
                this.isItemSelected = isSelected;
            }
            
            if (!this._disableStripes) {
                // Update the "even" status. Used for striping the virtual rows.
                const newEvenState = rowIndex % 2 === 0;
                const hasChangedEven = this._even !== newEvenState;
                if (hasChangedEven) {
                    this._even = newEvenState;
                    if (this._even) {
                        this.element.addClass("even");
                    } else {
                        this.element.removeClass("even");
                    }
                }
            }

            // Move it to its proper position.
            const desiredNewRowY = rowIndex * this._height;
            this.setElementTop(desiredNewRowY);
        }
    }

    reset() {
        this._item = null;
        this.isItemSelected = false;
        this.setElementTop(-9999);
        this._index = -1;
        this._even = null;
        this.element.text("");
        this.element.removeClass("selected");
    }

    private createCellsHtml(item: Object, columns: virtualColumn[], isSelected: boolean, sortColumnIndex: number): string {
        return columns.map((c, idx) => c.renderCell(item, isSelected, sortColumnIndex === idx))
            .join("");
    }

    private setElementTop(val: number) {
        this._top = val;
        this.element.css("top", val);
    }
}

export = virtualRow;
