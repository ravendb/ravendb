/// <reference path="../../../typings/tsd.d.ts"/>

import virtualRow = require("widgets/virtualGrid/virtualRow");

class shiftSelectionHandler implements disposable {
    private static readonly selectionPreviewClass = "selection-preview";

    private shiftSelectStartIndexCandidate: number = null;

    private readonly gridId: string;
    private readonly virtualRowsProvider: () => virtualRow[];
    private readonly $gridElement: JQuery<HTMLElement>;
    private readonly canHighlight: (start: number, end: number) => boolean;
    private renderedHintFor: [number, number] = null;
    private moveHandler: disposable = null;

    constructor(gridId: string, rowsProvider: () => virtualRow[], canHighlight: (start: number, end: number) => boolean) {
        this.gridId = gridId;
        this.virtualRowsProvider = rowsProvider;
        this.canHighlight = canHighlight;

        this.$gridElement = $(document.querySelector("#" + this.gridId)) as JQuery<HTMLElement>;
    }

    get selectionRange(): [number, number] {
        return this.renderedHintFor;
    }

    init() {
        const $document = $(document);

        $document.on("keydown." + this.gridId, (e: JQuery.TriggeredEvent) => {
            if (_.isNumber(this.shiftSelectStartIndexCandidate) && e.shiftKey) {
                if (!this.moveHandler) {
                    this.moveHandler = this.createShiftSelectionHandler();
                    this.updateHints(e);
                }
            }
        });

        $document.on("keyup." + this.gridId, () => {
            if (this.moveHandler) {
                this.moveHandler.dispose();
                this.moveHandler = null;
            }
        });
    }

    lastShiftIndex(idx: number) {
        this.shiftSelectStartIndexCandidate = idx;
    }

    private createShiftSelectionHandler(): disposable {
        this.$gridElement.on("mousemove.shift", (e: JQuery.TriggeredEvent) => this.updateHints(e));

        return {
            dispose: () => {
                this.syncSelectionHint(null, null);
                this.$gridElement.off("mousemove.shift");
            }
        }
    }

    private updateHints(e: JQuery.TriggeredEvent) {
        if (!e.shiftKey && this.moveHandler) {
            this.moveHandler.dispose();
            this.moveHandler = null;
            return;
        }

        let newHintFor: virtualRow = null;

        const $target = $(e.target);
        const $row = $target.closest(".virtual-row");
        if ($row.length) {
            const row = this.virtualRowsProvider().find(x => x.element[0] === $row[0]);
            newHintFor = row;
        } else {
            newHintFor = null;
        }

        this.syncSelectionHint(this.shiftSelectStartIndexCandidate, newHintFor ? newHintFor.index : null);
    }

    private syncSelectionHint(startIdx: number, endIdx: number) {
        if (!_.isNull(startIdx) && !_.isNull(endIdx)) {
            if (startIdx > endIdx) {
                [endIdx, startIdx] = [startIdx, endIdx];
            }

            if (!this.canHighlight(startIdx, endIdx)) {
                startIdx = null;
                endIdx = null;
            }
        }

        if (_.isNull(startIdx) || _.isNull(endIdx)) {
            if (this.renderedHintFor) {
                this.clearSelectionPreview();
                this.renderedHintFor = null;
            }
        } else {
            if (this.renderedHintFor === null || this.renderedHintFor[0] !== startIdx || this.renderedHintFor[1] !== endIdx) {
                this.generateSelectionPreview(startIdx, endIdx);
            }
            this.renderedHintFor = [startIdx, endIdx];
        }
    }

    private clearSelectionPreview() {
        this.virtualRowsProvider().forEach(row => {
            row.element.removeClass(shiftSelectionHandler.selectionPreviewClass);
        });
    }

    private generateSelectionPreview(startIdx: number, endIdx: number) {
        this.virtualRowsProvider().forEach(row => {
            row.element.toggleClass(shiftSelectionHandler.selectionPreviewClass, startIdx <= row.index && row.index <= endIdx);
        });
    }


    dispose() {
        const $document = $(document);
        $document.off("keydown." + this.gridId);
        $document.off("keyup." + this.gridId);
    }
}

export = shiftSelectionHandler;
