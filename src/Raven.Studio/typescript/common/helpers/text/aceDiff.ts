/// <reference path="../../../../typings/tsd.d.ts" />

import diff = require("diff");

type gapItem = {
    firstLine: number;
    emptyLinesCount: number;
}

type foldItem = {
    firstLine: number;
    lines: number;
}

class aceDiffEditor {
    static foldClassName = "diff_fold";
    
    private highlights: number[];
    private folds = [] as foldItem[];
    private readonly editor: AceAjax.Editor;
    private readonly mode: "left" | "right";
    private readonly gutterClass: string;
    private readonly markerClass: string;
    private previousAceMode: string;
    private onModeChange: () => void;
    
    private onScroll: (scroll: any) => void;
    private onFold: (foldEvent: any) => void;
    private markers = [] as number[];
    private widgets = [] as any[];
    
    private gutters = [] as Array<{ row: number, className: string, idx?: number }>;
    
    constructor(editor: AceAjax.Editor, mode: "left" | "right", gutterClass: string, markerClass: string) {
        this.editor = editor;
        this.mode = mode;
        this.gutterClass = gutterClass;
        this.markerClass = markerClass;
        
        this.initEditor();
    }

    getAllLines() {
        return this.getSession().getDocument().getAllLines();
    }

    private initEditor() {
        const session = this.getSession() as any;
        if (!session.widgetManager) {
            const LineWidgets = ace.require("ace/line_widgets").LineWidgets;
            session.widgetManager = new LineWidgets(session);
            session.widgetManager.attach(this.editor);
        }
        
        this.previousAceMode = this.getSession().getMode().$id;

        this.onModeChange = () => this.modeChanged();
        this.getSession().on("changeMode", this.onModeChange);
    }

    private modeChanged() {
        const mode = this.getSession().getMode();
        
        if (mode.$id === "ace/mode/raven_document_diff") {
            this.getSession().foldAll();
        }
    }
    
    getSession() {
        return this.editor.getSession();
    }
    
    getHighlightsCount() {
        return this.highlights.length;
    }
    
    update(patch: diff.IUniDiff, gaps: gapItem[]) {
        this.widgets = this.applyLineGaps(this.editor, gaps);
        this.highlights = this.findLinesToHighlight(patch.hunks, this.mode);
        this.folds = this.findFolds(this.highlights, gaps);
        this.decorateGutter(this.editor, this.gutterClass, this.highlights);
        this.createLineMarkers();
        this.addFolds();

        this.getSession().setMode("ace/mode/raven_document_diff");
    }
    
    private addFolds() {
        const session = this.getSession();
        this.folds.forEach((fold, idx) => {
            const linesClass = "diff_l_" + fold.lines;
            
            this.addDisposableGutterDecoration(session, fold.firstLine, aceDiffEditor.foldClassName, idx);
            this.addDisposableGutterDecoration(session, fold.firstLine, linesClass);
        })
    }
    
    private findFolds(highlightedLines: Array<number>, gaps: gapItem[]): Array<foldItem> {
        const totalLines = this.editor.getSession().getDocument().getLength();
        const bits = new Array(totalLines).fill(0);
        
        const context = 3;
        
        gaps.forEach(gap => {
            const l = gap.firstLine - 1;
            for (let i = Math.max(l - context, 0); i < Math.min(l + context, totalLines); i++) {
                bits[i] = 1;
            }
        });
        
        highlightedLines.forEach(l => {
            l = l - 1;
            for (let i = Math.max(l - context, 0); i < Math.min(l + context + 1, totalLines); i++) {
                bits[i] = 1;
            }
        });
        
        let inFold = false;
        let foldStart = -1;
        
        const result = [] as Array<foldItem>;
        for (let i = 0; i < totalLines; i++) {
            if (bits[i] === 0 && !inFold) {
                foldStart = i;
                inFold = true;
            }
            
            if (bits[i] === 1 && inFold) {
                result.push({
                    firstLine: foldStart,
                    lines: i - foldStart
                });
                inFold = false;
            }
        }
        
        if (inFold) {
            result.push({
                firstLine: foldStart,
                lines: totalLines - foldStart
            })
        }
        
        return result;
    }

    private createLineMarkers() {
        const marker = this.createLineHighlightMarker(this.markerClass, () => this.highlights);
        this.getSession().addDynamicMarker(marker, false);
        this.markers.push(marker.id);
    }

    private createLineHighlightMarker(className: string, linesProvider: () => Array<number>) {
        const AceRange = ace.require("ace/range").Range;

        return {
            id: undefined as number,
            update: (html: string[], marker: any, session: AceAjax.IEditSession, config: any) => {
                const lines = linesProvider();

                lines.forEach(line => {
                    const range = new AceRange(line - 1, 0, line - 1, Infinity);
                    if (range.clipRows(config.firstRow, config.lastRow).isEmpty()) {
                        return;
                    }

                    const screenRange = range.toScreenRange(session);
                    marker.drawScreenLineMarker(html, screenRange, className, config);
                });
            }
        }
    }

    private decorateGutter(editor: AceAjax.Editor, className: string, rows: Array<number>) {
        for (let i = 0; i < rows.length; i++) {
            this.addDisposableGutterDecoration(editor.getSession(), rows[i] - 1, className);
        }
    }
    
    private addDisposableGutterDecoration(session: AceAjax.IEditSession, row: number, className: string, idx?: number) {
        session.addGutterDecoration(row, className);
        
        this.gutters.push({
            className: className,
            row: row,
            idx: idx
        });
    }

    private findLinesToHighlight(hunks: diff.IHunk[], mode: "left" | "right") {
        const ignoreLinesStartsWith = mode === "left" ? "+" : "-";
        const takeLinesStartsWith = mode === "left" ? "-" : "+";

        const result = [] as Array<number>;
        hunks.forEach(hunk => {
            const startLine = mode === "left" ? hunk.oldStart : hunk.newStart;

            const filteredLines = hunk.lines.filter(x => !x.startsWith(ignoreLinesStartsWith));
            for (let i = 0; i < filteredLines.length; i++) {
                const line = filteredLines[i];
                if (line.startsWith(takeLinesStartsWith)) {
                    result.push(startLine + i);
                }
            }
        });
        return result;
    }

    private applyLineGaps(editor: AceAjax.Editor, gaps: Array<gapItem>) {
        const dom = ace.require("ace/lib/dom");
        const widgetManager = editor.getSession().widgetManager;
        const lineHeight = editor.renderer.layerConfig.lineHeight;

        return gaps.map(gap => {
            const element = dom.createElement("div") as HTMLElement;
            element.className = "difference_gap";
            element.style.height = gap.emptyLinesCount * lineHeight + "px";

            const widget = {
                row: gap.firstLine - 2,
                fixedWidth: true,
                coverGutter: false,
                el: element,
                type: "diffGap"
            };

            widgetManager.addLineWidget(widget);

            return widget;
        });
    }

    private cleanupGutter(editor: AceAjax.Editor) {
        const session = editor.getSession();
        for (let i = 0; i < this.gutters.length; i++) {
            const toClean = this.gutters[i];
            session.removeGutterDecoration(toClean.row, toClean.className);
        }
        
        this.gutters = [];
    }
    
    synchronizeScroll(secondEditor: aceDiffEditor) {
        this.onScroll = scroll => {
            const otherSession = secondEditor.getSession();
            if (scroll !== otherSession.getScrollTop()) {
                otherSession.setScrollTop(scroll || 0);
            }
        };
        
        this.getSession().on("changeScrollTop", this.onScroll);
    }
    
    synchronizeFolds(secondEditor: aceDiffEditor) {
        this.onFold = e => {
            const action = e.action;
            const startLine = e.data.start.row;
            
            const fold = this.gutters.find(x => x.row === startLine && x.className === aceDiffEditor.foldClassName);
            
            if (fold) {
                switch (action) {
                    case "add":
                        secondEditor.addFold(fold.idx);
                        break;
                    case "remove":
                        secondEditor.removeFold(fold.idx);
                        break;
                }
            }
        };
        
        this.getSession().on("changeFold", this.onFold);
    }
    
    addFold(idx: number) {
        const gutter = this.gutters.find(x => x.idx === idx && x.className === aceDiffEditor.foldClassName);
        
        const existingFold = this.getSession().getFoldAt(gutter.row, 0);
        if (existingFold) {
            return;
        }
        
        const range = this.getSession().getFoldWidgetRange(gutter.row);
        
        this.getSession().addFold("...", range);
    }
    
    removeFold(idx: number) {
        const gutter = this.gutters.find(x => x.idx === idx && x.className === aceDiffEditor.foldClassName);
        if (gutter) {
            const fold = this.getSession().getFoldAt(gutter.row, 0);
            if (fold) {
                this.getSession().removeFold(fold);
            }
        }
    }
    
    destroy() {
        if (this.onScroll) {
            this.getSession().off("changeScrollTop", this.onScroll);
            this.onScroll = null;
        }
        
        if (this.onFold) {
            this.getSession().off("changeFold", this.onFold);
            this.onFold = null;
        }
        
        this.cleanupGutter(this.editor);
        
        this.highlights = [];
        
        this.markers.forEach(marker => this.getSession().removeMarker(marker));
        this.markers = [];
        
        this.widgets.forEach(widget => this.getSession().widgetManager.removeLineWidget(widget));

        this.getSession().off("changeMode", this.onModeChange);
        
        this.getSession().setMode(this.previousAceMode);
    }
}

class aceDiff {
    
    private readonly leftEditor: aceDiffEditor;
    private readonly rightEditor: aceDiffEditor;
    
    additions = ko.observable<number>(0);
    deletions = ko.observable<number>(0);
    identicalContent: KnockoutObservable<boolean>;
    
    constructor(leftEditor: AceAjax.Editor, rightEditor: AceAjax.Editor) {
        this.leftEditor = new aceDiffEditor(leftEditor, "left", "ace_removed", "ace_code-removed");
        this.rightEditor = new aceDiffEditor(rightEditor, "right", "ace_added", "ace_code-added");
        
        this.identicalContent = ko.pureComputed(() => {
            const a = this.additions();
            const d = this.deletions();
            return a === 0 && d === 0;
        });
        
        this.computeDifference();
        this.leftEditor.synchronizeScroll(this.rightEditor);
        this.rightEditor.synchronizeScroll(this.leftEditor);
        
        this.leftEditor.synchronizeFolds(this.rightEditor);
        this.rightEditor.synchronizeFolds(this.leftEditor);
        
        //initial sync:
        this.rightEditor.getSession().setScrollTop(this.leftEditor.getSession().getScrollTop());
    }

    private computeDifference() {
        const leftLines = this.leftEditor.getAllLines();
        const rightLines = this.rightEditor.getAllLines();

        const patch = diff.structuredPatch("left", "right",
            leftLines.join("\r\n"), rightLines.join("\r\n"),
            null, null, {
                context: 0
            });

        const leftGaps = patch.hunks
            .filter(x => x.oldLines < x.newLines)
            .map(hunk => ({
                emptyLinesCount: hunk.newLines - hunk.oldLines,
                firstLine: hunk.oldStart + hunk.oldLines
            } as gapItem));

        const rightGaps = patch.hunks
            .filter(x => x.oldLines > x.newLines)
            .map(hunk => ({
                emptyLinesCount: hunk.oldLines - hunk.newLines,
                firstLine: hunk.newStart + hunk.newLines
            } as gapItem));
        
        this.leftEditor.update(patch, leftGaps);
        this.rightEditor.update(patch, rightGaps);

        this.additions(this.rightEditor.getHighlightsCount());
        this.deletions(this.leftEditor.getHighlightsCount());
    }
    
    destroy() {
        this.leftEditor.destroy();
        this.rightEditor.destroy();
    }
}

export = aceDiff;
