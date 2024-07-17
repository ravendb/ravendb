/// <reference path="../../../typings/tsd.d.ts"/>

import listViewController = require("widgets/listView/listViewController");
import virtualListRow = require("widgets/listView/virtualListRow");
import { sortBy } from "common/typeUtils";

/**
 * This list view is optimized to handle following lists:
 *     - append only (you can not edit existing records)
 *     - external height provider must be supplied
 *     - it doesn't support async loading, however elements can be pushed multiple times
 */
class listView<T> {

    private items = new Map<number, T>();
    private cumulativeItemsHeight = new Map<number, number>();
    
    virtualHeight = ko.observable<number>(0);
    private listId: string;
    private emptyTemplate: string = null;
    private isLoading = ko.observable(false);

    private itemHtmlProvider: (item: T) => string;
    private itemHeightProvider: (item: T, row: virtualListRow<T>) => number;
    private controller: listViewController<T>;

    private virtualRows: virtualListRow<T>[] = []; // These are the fixed number of elements that get displayed on screen. Each virtual row displays an element from .items array. As the user scrolls, rows will be recycled to represent different items.
    private $listElement: JQuery<HTMLElement>;
    private $viewportElement: JQuery<HTMLElement>;
    private listElementHeight: number;
    private heightMeasureVirtualRow: virtualListRow<T>;

    private isListVisible = false;
    
    private scrollAnimationFrameHandle = 0;

    private static readonly viewportSelector = ".viewport";
    private static readonly viewportScrollerSelector = ".viewport-scroller";

    private emptyResult = ko.observable(true);

    constructor(params: { controller: KnockoutObservable<listViewController<T>>, emptyTemplate: string, itemHtmlProvider: (item: T) => string, itemHeightProvider: (item: T, row: virtualListRow<T>) => number}) {
        this.listId = _.uniqueId("vl_");

        this.initController();

        if (params.controller) {
            params.controller(this.controller);
        }

        if (params.emptyTemplate) {
            this.emptyTemplate = params.emptyTemplate;
        }
        
        this.itemHtmlProvider = params.itemHtmlProvider;
        this.itemHeightProvider = params.itemHeightProvider || this.measureHeight;
        this.heightMeasureVirtualRow = new virtualListRow(params.itemHtmlProvider);
    }
    
    private initController() {
        this.controller = {
            reset: () => this.resetItems(),
            pushElements: (items: T[]) => this.onNewElements(items),
            getItems: () => this.items,
            scrollDown: () => this.scrollDown(),
            getTotalCount: () => this.items.size
        }
    }

    // Called by Knockout once the grid has been rendered.
    private afterRender() {
        this.initializeUIElements();
    }

    private initializeUIElements() {
        this.$listElement = this.findListViewElement();
        this.listElementHeight = this.$listElement.height();
        this.$viewportElement = this.$listElement.find(listView.viewportSelector);
        this.initializeVirtualRows();
        this.$viewportElement.on("scroll", () => this.listScrolled());
    }
    
    private onNewElements(items: T[]) {
        this.emptyResult(false);
        
        let virtualHeight = this.virtualHeight();
        for (let i = 0; i < items.length; i++) {
            const item = items[i];

            const itemIdx = this.items.size;

            this.items.set(itemIdx, item);

            const itemHeight = this.itemHeightProvider(item, this.heightMeasureVirtualRow);
            this.cumulativeItemsHeight.set(itemIdx, virtualHeight + itemHeight);
            virtualHeight += itemHeight;
        }
        this.virtualHeight(virtualHeight);
        
        
        this.render();
    }
    
    private measureHeight(item: T): number {
        this.heightMeasureVirtualRow.populate(item, 0, -20000, undefined);
        return this.heightMeasureVirtualRow.element.height();
    }
    
    private scrollDown() {
        const viewPort = this.$viewportElement[0];
        if (viewPort) {
            viewPort.scrollTop = viewPort.scrollHeight;    
        }
    }
    
    private getItemHeight(idx: number) {
        if (idx === 0) {
            return this.cumulativeItemsHeight.get(0);
        }
        
        return this.cumulativeItemsHeight.get(idx) - this.cumulativeItemsHeight.get(idx - 1);
    }
    
    private initializeVirtualRows() {
        this.virtualRows = this.createVirtualRows();
        this.$viewportElement
            .find(listView.viewportScrollerSelector)
            .empty()
            .append(this.virtualRows.map(r => r.element[0]))
            .append(this.heightMeasureVirtualRow.element[0]);
    }

    private createVirtualRows(): virtualListRow<T>[] {
        const height = Math.max(100, this.listElementHeight);
        const minElementHeight = 20;
        const maxRowsNeededToCoverViewport = Math.ceil(height / minElementHeight);
        const desiredRowCount = maxRowsNeededToCoverViewport * 2;
        const rows: virtualListRow<T>[] = [];
        rows.length = desiredRowCount;
        for (let i = 0; i < desiredRowCount; i++) {
            rows[i] = new virtualListRow(this.itemHtmlProvider);
        }

        return rows;
    }

    private listScrolled() {
        if (this.items.size) {
            window.cancelAnimationFrame(this.scrollAnimationFrameHandle);
            this.scrollAnimationFrameHandle = window.requestAnimationFrame(() => this.render());
        }
    }

    private render() {
        // The list may not be visible if the results returned quickly and we haven't finished initializing the UI.
        // In such a case, we queue up a render to occur later.
        if (this.checkListViewVisibility()) {
            this.checkForUpdatedListHeight();
            this.layoutVirtualRowPositionsAndPopulate();
            this.sortRowsByTopPosition();
        } else {
            throw new Error("List View is not visible!");
        }
    }
  
    private findListViewElement(): JQuery<HTMLElement> {
        const element = $(document.querySelector<HTMLElement>("#" + this.listId));
        if (element.length === 0) {
            throw new Error("Couldn't find list view element with ID " + this.listId);
        }

        return element;
    }
  
    private checkForUpdatedListHeight(): number {
        const oldHeight = this.listElementHeight;
        const newHeight = this.$listElement.height();
        this.listElementHeight = newHeight;

        // If the grid grew taller, we may need more virtual rows.
        if (newHeight > oldHeight) {
            this.initializeVirtualRows();
        }
        
        return newHeight;
    }
    
    private checkListViewVisibility(): boolean {
        // If we've already determined the grid is visible, roll with that.
        if (this.isListVisible) {
            return true;
        }
        // Grid hasn't yet become visible. Do the more expensive JQuery call.
        else if (this.$listElement) {
            return (this.isListVisible = this.$listElement.is(":visible"));
        }
        return false;
    }

    private layoutVirtualRowPositionsAndPopulate() {
        // This is hot path, called multiple times when scrolling. 
        // Keep allocations to a minimum.

        // Determine the view port.
        const scrollTop = this.$viewportElement.scrollTop();
        const scrollBottom = scrollTop + this.listElementHeight;
        
        const [firstVisibleIdx, lastVisibleIdx] = this.visibleItemsRange(scrollTop, scrollBottom);

        const usedRows: virtualListRow<T>[] = [];
        const missingIdx: number[] = [];
        
        // first try to reuse existing row - useful when user uses mouse scroll
        
        let currentIdx = firstVisibleIdx;
        while (currentIdx <= lastVisibleIdx) {
            const rowAtPosition = this.findRowForIdx(currentIdx);
            if (rowAtPosition) {
                usedRows.push(rowAtPosition);
                this.populate(currentIdx, rowAtPosition);
            } else {
                missingIdx.push(currentIdx);
            }
            
            currentIdx++;
        }
        
        // now fill remaining rows
        
        if (missingIdx.length) {
            // reuse rows which are not in rowsToUse
            for (let i = 0; i < this.virtualRows.length; i++) {
                const row = this.virtualRows[i];
                
                if (!_.includes(usedRows, row)) {
                    const idx = missingIdx.pop();
                    
                    this.populate(idx,  row);
                    
                    if (!missingIdx.length) {
                        break;
                    }
                }
            }
        }
    }
    
    private populate(dataIdx: number, row: virtualListRow<T>) {
        const itemHeight = this.getItemHeight(dataIdx);
        row.populate(this.items.get(dataIdx), dataIdx, this.cumulativeItemsHeight.get(dataIdx) - itemHeight, itemHeight);
    }
    
    private sortRowsByTopPosition() {
        const parent = this.$viewportElement
            .find(listView.viewportScrollerSelector)[0];

        let children: HTMLElement[] = [];            
            
        for (let i = parent.children.length - 1; i >= 0; i--) {
            children.push(parent.children.item(i) as HTMLElement);
            parent.removeChild(parent.childNodes[i]);
        }

        children = sortBy(children, x => parseInt(x.style.top));
        
        children.forEach(c => {
            parent.appendChild(c);
        })
    }
    
    /*
    Gets first and last items index which will be visible on screen
     */
    private visibleItemsRange(yStart: number,  yEnd: number) : [number, number] {
        yEnd = Math.min(yEnd, this.virtualHeight());
        
        let minIdx = 0;
        let maxIdx = this.items.size;
        
        while (minIdx !== maxIdx) {
            const idxToTest = Math.floor((minIdx + maxIdx) / 2);
            
            const itemEnd = this.cumulativeItemsHeight.get(idxToTest);
            const itemStart = itemEnd - this.getItemHeight(idxToTest);
            
            if (itemStart <= yStart && yStart < itemEnd) {
                minIdx = maxIdx = idxToTest;
                break;
            }
            
            if (yStart < itemStart) {
                // search below
                maxIdx = idxToTest;
            } else {
                // search above
                minIdx = idxToTest;
            }
        }
        
        const firstIdx = minIdx;
        let lastIdx = 0;
        
        for (let i = firstIdx; i < this.items.size; i++) {
            const itemEnd = this.cumulativeItemsHeight.get(i);

            if (itemEnd >= yEnd) {
                lastIdx = i;
                break;
            }
        }

        
        return [firstIdx, lastIdx];
    }
    
    private findRowForIdx(currrentIdx: number): virtualListRow<T> | null {
        for (let i = 0; i < this.virtualRows.length; i++) {
            const vRow = this.virtualRows[i];
            
            if (vRow.index === currrentIdx) {
                return vRow;
            }
        }
        
        return null;
    }

    private resetItems() {
        this.items.clear();
        this.cumulativeItemsHeight.clear();
        this.emptyResult(true);
        this.virtualHeight(0);
        
        this.isLoading(false);
        this.$viewportElement.scrollTop(0);
        this.virtualRows.forEach(r => r.reset());
    }

    static install() {
        const componentName = "list-view";
        if (!ko.components.isRegistered(componentName)) {
            ko.components.register(componentName, {
                viewModel: listView,
                template: `
<div class="list-view flex-window stretch" data-bind="attr: { id: listId }">
    <div class="absolute-center loading" data-bind="visible: isLoading"><div class="global-spinner"></div></div>
    <div class="viewport flex-window-scroll">
        <div class="viewport-scroller" data-bind="style: { height: virtualHeight() + 'px'}, template: { afterRender: afterRender.bind($data) }">
        </div>
    </div>
    <div class="absolute-center" data-bind="visible: !isLoading() && emptyTemplate && emptyResult(), if: emptyTemplate">
        <div data-bind="template: emptyTemplate"></div>
    </div>
</div>
`
            });
        }
    }
}

export = listView;
