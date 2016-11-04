import virtualColumn = require("widgets/virtualGrid/virtualColumn");
import textCellTemplate = require("widgets/virtualGrid/textCellTemplate");

/**
 * A virtual row. Contains an element displayed as a row in the grid. Gets recycled as the grid scrolls in order to create and manage fewer elements.
 */
class VirtualRow {
    private item: Object | null = null; // The last item populated into this virtual row.
    private isItemSelected = false;
    readonly element: JQuery;
    private _top = -9999;
    private _index = -1;
    private even = true;

    public static readonly height = 36;

    constructor() {
        this.element = $(`<div class="virtual-row" style="height: ${VirtualRow.height}px; top: ${this.top}px"></div>`);
    }

    get top(): number {
        return this._top;
    }

    /**
     * Gets the index of the row this virtual row is displaying.
     */
    get index(): number {
        return this._index;
    }

    get hasData(): boolean {
        return !!this.item;
    }

    isOffscreen(scrollTop: number, scrollBottom: number) {
        const top = this.top;
        const bottom = top + VirtualRow.height;
        return top > scrollBottom || bottom < scrollTop;
    }

    dataLoadError(error: any) {
        this.element.text(`Unable to load data: ${JSON.stringify(error)}`);
    }

    populate(item: Object | null, rowIndex: number, isSelected: boolean, columns: virtualColumn[]) {
        // Optimization: don't regenerate this row HTML if nothing's changed since last render.
        const alreadyDisplayingData = !!item && this.item === item && this._index === rowIndex && this.isItemSelected === isSelected;
        if (!alreadyDisplayingData) {
            this.item = item;
            this._index = rowIndex;

            // If we have data, fill up this row content.
            if (item) {
                const html = this.createCellsHtml(item, columns, isSelected);
                this.element.html(html);
            } else {
                this.element.text("Loading...");
            }

            // Update the selected status. Displays as a different row color.
            const hasChangedSelectedStatus = this.isItemSelected !== isSelected;
            if (hasChangedSelectedStatus) {
                this.element.toggleClass("selected");
                this.isItemSelected = isSelected;
            }

            // Update the "even" status. Used for striping the virtual rows.
            const hasChangedEven = this.even !== (rowIndex % 2 === 0);
            if (hasChangedEven) {
                this.element.toggleClass("even");
                this.even = !this.even;
            }

            // Move it to its proper position.
            const desiredNewRowY = rowIndex * VirtualRow.height;
            this.setElementTop(desiredNewRowY);
        }
    }

    private createCellsHtml(item: Object, columns: virtualColumn[], isSelected: boolean): string {
        const cellsHtml = columns
            .map(c => {
                if (!c.template) {
                    c.template = new textCellTemplate();
                }
                return `<div class="cell ${c.template.className}" style="width: ${c.width}">${c.template.getHtml(item, c.dataMemberName, isSelected)}</div>`;
            });
        return cellsHtml.join("");
    }

    private setElementTop(val: number) {
        this._top = val;
        this.element.css("top", val);
    }
}

export = VirtualRow;
