/// <reference path="../../../typings/tsd.d.ts"/>

/**
 * A virtual row. Contains an element displayed as a row in the list view. Gets recycled as the list view scrolls in order to create and manage fewer elements.
 */
class virtualListRow<T> {
    static readonly defaultTopPosition = -9999;
    
    private _item: T | null = null; // The last item populated into this virtual list row.
    readonly element: JQuery;
    private _top = virtualListRow.defaultTopPosition;
    private _index = -1;
    private _even: boolean | null = null;

    private _height: number;
    
    private _htmlProvider: (item: T) => string;
    
    constructor(htmlProvider: (item: T) => string) {
        this.element = $(`<div class="virtual-row" style="top: ${this.top}px"></div>`);
        this._htmlProvider = htmlProvider;
    }

    get top(): number {
        return this._top;
    }
    
    get height(): number {
        return this._height;
    }

    get data(): T {
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

    populate(item: T | null, rowIndex: number, top: number, height: number) {
        // Optimization: don't regenerate this row HTML if nothing's changed since last render.
        const alreadyDisplayingData = !!item && this._item === item && this._index === rowIndex;
        if (!alreadyDisplayingData) {
            this._item = item;
            this._index = rowIndex;

            // If we have data, fill up this row content.
            if (item) {
                this.element.html(this._htmlProvider(item));
            } else {
                this.element.text("");
            }

            // Update the "even" status. Used for striping the virtual rows.
            const newEvenState = rowIndex % 2 === 0;
            const hasChangedEven = this._even !== newEvenState;
            if (hasChangedEven) {
                this._even = newEvenState;
                this.element.toggleClass("even", this._even);
            }

            // Move it to its proper position.
            this.setElementTop(top);
            this.setElementHeight(height);
        }
    }

    reset() {
        this._item = null;
        this.setElementTop(-9999);
        this._index = -1;
        this._even = null;
        this.element.text("");
    }

    private setElementTop(val: number) {
        this._top = val;
        this.element.css("top", val + "px");
    }
    
    private setElementHeight(val: number) {
        this._height = val;
        this.element.css("height", val === undefined ? "auto" : val + "px");
    }
}

export = virtualListRow;
