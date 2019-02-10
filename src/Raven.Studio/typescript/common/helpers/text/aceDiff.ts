/// <reference path="../../../../typings/tsd.d.ts" />

import diff = require("diff");

type gapItem = {
    firstLine: number;
    emptyLinesCount: number;
}

class aceDiffEditor {
    highlights: number[];
    editor: AceAjax.Editor;
    mode: "left" | "right";
    gutterClass: string;
    markerClass: string;
    
    onScroll: (scroll: any) => void;
    markers = [] as number[];
    widgets = [] as any[];
    
    constructor(editor: AceAjax.Editor, mode: "left" | "right", gutterClass: string, markerClass: string) {
        this.editor = editor;
        this.mode = mode;
        this.gutterClass = gutterClass;
        this.markerClass = markerClass;
        
        this.initEditor();
    }

    getAllLines() {
        return this.editor.getSession().getDocument().getAllLines();
    }

    private initEditor() {
        const session = this.editor.getSession() as any;
        if (!session.widgetManager) {
            const LineWidgets = ace.require("ace/line_widgets").LineWidgets;
            session.widgetManager = new LineWidgets(session);
            session.widgetManager.attach(this.editor);
        }

        // we want to manage folds manually 
        this.editor.getSession().setFoldStyle("manual");
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
        this.decorateGutter(this.editor, this.gutterClass, this.highlights);
        this.createLineMarkers();
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
            editor.getSession().addGutterDecoration(rows[i] - 1, className);
        }
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

    private cleanupGutter(editor: AceAjax.Editor, className: string, lineNumbers: number[]) {
        const session = editor.getSession();
        lineNumbers.forEach(line => session.removeGutterDecoration(line - 1, className));
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
    
    destroy() {
        if (this.onScroll) {
            this.getSession().off("changeScrollTop", this.onScroll);
            this.onScroll = null;
        }
        
        this.cleanupGutter(this.editor, this.gutterClass, this.highlights);
        
        this.highlights = [];
        
        this.markers.forEach(marker => this.getSession().removeMarker(marker));
        
        this.markers = [];
        
        this.widgets.forEach(widget => this.getSession().widgetManager.removeLineWidget(widget));
        
        this.getSession().setFoldStyle("markbegin");
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
