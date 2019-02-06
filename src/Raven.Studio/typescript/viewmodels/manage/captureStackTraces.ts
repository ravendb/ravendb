import viewModelBase = require("viewmodels/viewModelBase");
import d3 = require("d3");
import captureLocalStackTracesCommand = require("commands/maintenance/captureLocalStackTracesCommand");
import captureClusterStackTracesCommand = require("commands/maintenance/captureClusterStackTracesCommand");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import copyToClipboard = require("common/copyToClipboard");
import fileDownloader = require("common/fileDownloader");
import fileImporter = require("common/fileImporter");

type stackFrame = {
    short: string;
    full: string;
}

class stackInfo {
    static boxPadding = 15;
    static lineHeight = 20;
    static headerSize = 50;

    static shortName(v: string) {
        const withoutArgs = v.replace(/\(.*?\)/g, '');
        if (withoutArgs.includes('+')) {
            return withoutArgs.replace(/.*\.(.*\+.*)/, '$1');
        } else {
            return withoutArgs.replace(/.*\.([^\.]+\.[^\.]+)$/, '$1');
        }
    }

    static isUserCode(line: string): boolean {
        return line.startsWith("Raven") || line.startsWith("Voron") || line.startsWith("Sparrow");
    }

    constructor(public threadIds: number[], public stackTrace: string[]) {
    }
    
    x: number;
    y: number;

    children: stackInfo[];
    parent: stackInfo;
    depth: number;

    boxHeight() {
        return this.stackTrace.length * stackInfo.lineHeight + 2 * stackInfo.boxPadding;
    }

    stackWithShortcuts() {
        return this.stackTrace.map(v => {
            return {
                short: stackInfo.shortName(v),
                full: v
            } as stackFrame;
        });
    }
    
    static for(raw: rawStackTraceResponseItem) {
        return new stackInfo(raw.ThreadIds, raw.StackTrace);
    }
}

class captureStackTraces extends viewModelBase {
    
    spinners = {
        loading: ko.observable<boolean>(false)
    };
    
    data: Array<rawStackTraceResponseItem> = [];
    
    clusterWideData = ko.observableArray<clusterWideStackTraceResponseItem>([]);
    selectedClusterWideData = ko.observable<clusterWideStackTraceResponseItem>();
    clusterWide = ko.observable<boolean>(false); 
    
    error = ko.observable<string>();

    isImport = ko.observable<boolean>(false);
    hasAnyData = ko.observable<boolean>(false);
    
    static maxBoxWidth = 280;
    static boxVerticalPadding = 100;
    static btnSize = 34;
    
    constructor() {
        super();
        this.bindToCurrentInstance("draw");
        
        this.initObservables();
    }
    
    private initObservables() {
        this.selectedClusterWideData.subscribe(data => {
            this.error(data.Error);
            
            if (data.Error) {
                this.clearGraph();
            } else {
                this.data = data.Stacks;
                this.draw(); 
            }
        })
    }
    
    private static splitAndCollateStacks(stacks: stackInfo[], parent: stackInfo): stackInfo[] {
        if (stacks.length === 1) {
            return stacks;
        }
        const grouped = d3.nest().key((d: stackInfo) => d.stackTrace[0]).entries(stacks);

        // for each group find common stack
        return grouped.map(kv => {
            const sharedStacks: stackInfo[] = kv.values;
            const minDepth = d3.min(sharedStacks, s => s.stackTrace.length);

            let depth = 0;
            for (; depth < minDepth; depth++) {
                if (_.some(sharedStacks, x => x.stackTrace[depth] !== sharedStacks[0].stackTrace[depth])) {
                    break;
                }
            }

            // extract shared stack:
            const sharedStack = new stackInfo([], sharedStacks[0].stackTrace.slice(0, depth));

            // remove shared stack from all stacks and recurse
            const strippedStacks = sharedStacks
                .map(s => new stackInfo(s.threadIds, s.stackTrace.slice(depth)))
                .filter(s => s.stackTrace.length > 0);
            
            sharedStack.children = captureStackTraces.splitAndCollateStacks(strippedStacks, sharedStack);
            sharedStack.threadIds = d3.merge(sharedStacks.map(s => s.threadIds));
            sharedStack.parent = parent;
            
            return sharedStack;
        });
    }
    
    private static cumulativeSumWithPadding(input: any[], padding: number) {
        let currentSum = 0;
        const output = [0];
        for (let i = 0; i < input.length; i++) {
            const offset = padding + input[i];
            output.push(currentSum + offset);
            currentSum += offset;
        }
        return output;
    }
    
    private updateGraph(roots: stackInfo[]) {
        this.clearGraph();
        
        const $container = $("#js-tracks-container");
        
        const svg = d3.select("#js-tracks-container")
            .append("svg")
            .attr("transform", "translate(0.5,0.5)")
            .attr("preserveAspectRatio", "xMinYMin slice")
            .style({
                width: $container.innerWidth() + "px",
                height: $container.innerHeight() + "px"
            })
            .attr("viewBox", "0 0 " + $container.innerWidth() + " " + $container.innerHeight());

        const svgDefs = svg.append("defs");

        const mainGroup = svg
            .append("g")
            .attr("class", "main-group");
        
        const innerGroup = mainGroup
            .append("g");
        
        const zoom = d3.behavior.zoom()
            .scale(0.5)
            .scaleExtent([0.1, 1.5])
            .on("zoom", () => this.zoom(innerGroup));

        mainGroup.call(zoom);
        
        zoom.event(mainGroup);

        innerGroup.append("rect")
            .attr("class", "overlay");

        const nodeSelector = innerGroup.selectAll(".node");
        const linkSelector = innerGroup.selectAll(".link");

        const fakeRoot: stackInfo = new stackInfo([], []);
        fakeRoot.children = roots;

        const treeLayout = d3.layout.tree<stackInfo>().nodeSize([captureStackTraces.maxBoxWidth + 20, 100]);
        const nodes = treeLayout.nodes(fakeRoot).filter(d => d.depth > 0);

        const maxBoxHeightOnDepth = d3.nest<stackInfo>()
            .key(d => d.depth.toString())
            .sortKeys(d3.ascending)
            .rollup((leaves: any[]) => d3.max(leaves, l => l.boxHeight()))
            .entries(nodes)
            .map(v => v.values);

        const cumulativeHeight = captureStackTraces.cumulativeSumWithPadding(maxBoxHeightOnDepth, captureStackTraces.boxVerticalPadding);

        const totalHeight = cumulativeHeight[cumulativeHeight.length - 1];
        const xExtent = d3.extent(nodes, (node: any) => node.x);
        xExtent[1] += captureStackTraces.maxBoxWidth; 
        const totalWidth = xExtent[1] - xExtent[0];

        d3.select(".overlay")
            .attr("width", totalWidth)
            .attr("height", totalHeight);

        const halfBoxShift = captureStackTraces.maxBoxWidth / 2 + 10; // little padding

        const xScale = d3.scale.linear()
            .domain([xExtent[0] - halfBoxShift, xExtent[1] - halfBoxShift])
            .range([0, totalWidth]);
        
        const yScale = d3.scale.linear()
            .domain([0, totalHeight])
            .range([totalHeight, 0]);

        const yDepthScale = d3.scale.linear().domain(d3.range(1, cumulativeHeight.length + 2, 1)).range(cumulativeHeight);

        const links = treeLayout.links(nodes)
            .map(link => {
                const targetY = yDepthScale(link.target.depth);
                const linkHeight = (captureStackTraces.boxVerticalPadding - stackInfo.headerSize) / 2;
    
                return {
                    source: {
                        x: link.source.x,
                        y: targetY - linkHeight,
                        y0: link.source.y,
                        depth: link.source.depth
                    },
                    target: {
                        x: link.target.x,
                        y: targetY,
                        y0: link.target.y
                    }
                }
        });

        const nodeSelectorWithData = nodeSelector.data(nodes);
        const linkSelectorWithData = linkSelector.data(links);

        const enteringNodes = nodeSelectorWithData
            .enter()
            .append("g") 
            .attr("class", "threadGroup")
            .attr("transform", d => "translate(" + xScale(d.x) + "," + yScale(yDepthScale(d.depth)) + ")");

        enteringNodes
            .filter((d: stackInfo) => d.children && d.children.length > 0)
            .append("line")
            .attr("class", d => "link level-" + d.depth)
            .attr("x1", 0)
            .attr("x2", 0)
            .attr("y1", d => -d.boxHeight() - stackInfo.headerSize)
            .attr("y2", d => -maxBoxHeightOnDepth[d.depth - 1] - stackInfo.headerSize / 2 - captureStackTraces.boxVerticalPadding/2);

        enteringNodes.append('rect')
            .attr('class', 'threadContainer')
            .attr('x', -captureStackTraces.maxBoxWidth / 2)
            .attr('y', d => -1 * d.boxHeight() - stackInfo.headerSize)
            .attr('width', captureStackTraces.maxBoxWidth)
            .attr('height', d => d.boxHeight() + stackInfo.headerSize);

        const clipPaths = svgDefs
            .selectAll('.stackClip')
            .data(nodes);
        
        clipPaths
            .enter()
            .append("clipPath")
            .attr('class', 'stackClip')
            .attr('id', (d, i) => 'stack-clip-path-' + i)
            .append('rect')
            .attr('x', -captureStackTraces.maxBoxWidth / 2)
            .attr('width', captureStackTraces.maxBoxWidth - stackInfo.boxPadding)
            .attr('y', d => -1 * d.boxHeight() - stackInfo.headerSize)
            .attr('height', d => d.boxHeight() + stackInfo.headerSize);
                
        enteringNodes
            .append("rect")
            .attr("class", "header")
            .attr("x", -1 * captureStackTraces.maxBoxWidth / 2)
            .attr("width", captureStackTraces.maxBoxWidth)
            .attr("y", d => -d.boxHeight() - stackInfo.headerSize)
            .attr("height", stackInfo.headerSize);

        const headerGroup = enteringNodes
            .append("g")
            .attr("transform", d => "translate(" + "-30" +"," + (-d.boxHeight() - 15) + ")");

        headerGroup
            .append("text")
            .attr("class", "count")
            .attr('text-anchor', 'end')
            .attr("x", -3)
            .text(d => d.threadIds.length);

        headerGroup
            .append("text")
            .attr("class", "title")
            .attr('text-anchor', 'start')
            .attr("x", 3)
            .text(d => this.pluralize(d.threadIds.length, "THREAD", "THREADS", true));
        
        const buttonGroup = enteringNodes
            .append("g")
            .attr("transform", d => "translate(" + (captureStackTraces.maxBoxWidth / 2) + "," + (-1 * d.boxHeight() - stackInfo.headerSize/2) + ")")
            .attr("class", "button");
        
        buttonGroup
            .append("rect")
            .attr("class", "btn-copy")
            .attr({
                x: -(stackInfo.headerSize - captureStackTraces.btnSize) / 2 - captureStackTraces.btnSize,
                y: -captureStackTraces.btnSize / 2,
                width: captureStackTraces.btnSize,
                height: captureStackTraces.btnSize
            })
            .on("click", d => this.onCopyStack(d))
            .append("title")
            .text("Copy stack to clipboard");
            
        
        buttonGroup
            .append("text")
            .attr("class", "icon-style copy")
            .html("&#xe943;")
            .attr("text-anchor", "middle")
            .attr("x", -stackInfo.headerSize / 2)
            .attr("y", 0);
        
        enteringNodes.filter(d => d.depth > 0).each(function (d: stackInfo, index: number) {
            const threadContainer = this;
            const offsetTop = d.boxHeight() - stackInfo.boxPadding - stackInfo.lineHeight/2;
            const framesContainer = d3.select(threadContainer)
                .append("g")
                .attr('class', 'traces')
                .style('clip-path', () => 'url(#stack-clip-path-' + index + ')');
            
            const stack = framesContainer
                .selectAll('.frame')
                .data(d.stackWithShortcuts().reverse());
            
            const reversedOriginalStack = d.stackTrace.reverse();
            
            stack
                .enter()
                .append("rect")
                .attr("class", "wireframe")
                .classed('own', (s, i) => stackInfo.isUserCode(reversedOriginalStack[i]))
                .attr("x", -captureStackTraces.maxBoxWidth / 2 + stackInfo.boxPadding)
                .attr("y", (d, i) => -offsetTop + stackInfo.lineHeight * i - stackInfo.lineHeight / 2)
                .attr("width", captureStackTraces.maxBoxWidth - 2 * stackInfo.boxPadding)
                .attr("height", stackInfo.lineHeight - 3);
            
            stack
                .enter()
                .append('text')
                .attr("class", "frame")
                .attr('x', -captureStackTraces.maxBoxWidth / 2 + stackInfo.boxPadding)
                .attr('y', (d, i) => -offsetTop + stackInfo.lineHeight * i)
                .text(d => d.short)
                .classed('own', (s, i) => stackInfo.isUserCode(reversedOriginalStack[i]))
                .append("title")
                .text(d => d.full);
        });

        const lineFunction = d3.svg.line()
            .x(x => xScale(x[0]))
            .y(y => yScale(y[1]))
            .interpolate("linear");
        
        linkSelectorWithData
            .enter()
            .append("path")
            .attr("class", d => "link level-" + d.source.depth)
            .attr("d", d => lineFunction([[d.source.x, d.source.y], [d.target.x, d.source.y], [d.target.x, d.target.y]]));
    }
    
    zoom(container: d3.Selection<void>) {
        const event = d3.event as d3.ZoomEvent;
        container.classed("wireframes", event.scale < 0.4);
        container.attr("transform", "translate(" + event.translate + ")scale(" + event.scale + ")");
    }
    
    private nodeTagToServerUrlMap(): dictionary<string> {
        const result = {} as dictionary<string>;
        clusterTopologyManager.default
            .topology()
            .nodes()
            .forEach(node => {
                result[node.tag()] = node.serverUrl();
            });
        
        return result;
    }
    
    captureStacks() {
        this.spinners.loading(true);
        this.clusterWideData([]);
        this.error(null);
        this.hasAnyData(false);
        
        const tagMapping = this.nodeTagToServerUrlMap();
        
        if (this.clusterWide()) {
            new captureClusterStackTracesCommand()
                .execute()
                .done(stacks => {
                    stacks.forEach(stack => {
                        if (stack.Stacks) {
                            this.reverseStacks(stack.Stacks);    
                        }
                        
                        const serverUrl = tagMapping[stack.NodeTag];
                        stack.NodeUrl = serverUrl || "<Unknown URL>";
                    });
                    this.clusterWideData(stacks);
                    this.selectedClusterWideData(stacks[0]);
                    this.hasAnyData(true);
                })
                .always(() => this.spinners.loading(false));
        } else {
            new captureLocalStackTracesCommand()
                .execute()
                .done(stacks => {
                    this.data = stacks;
                    this.hasAnyData(true);
                    this.reverseStacks(stacks);
                    this.draw();
                })
                .always(() => this.spinners.loading(false));
        }
    }
    
    draw() {
        const collatedStacks = captureStackTraces.splitAndCollateStacks(this.data.filter(x => x.StackTrace.length).map(x => stackInfo.for(x)), null);
        this.updateGraph(collatedStacks);
    }
    
    private clearGraph() {
        const $container = $("#js-tracks-container");

        $container.empty();
    }
    
    private reverseStacks(data: rawStackTraceResponseItem[]) {
        data.forEach(d => {
            d.StackTrace = d.StackTrace.reverse();
        });
    }
    
    private onCopyStack(data: stackInfo) {
        const stackFrames = [] as Array<string>;
        
        do {
            stackFrames.push(...data.stackTrace);
            data = data.parent;
        } while (data);
     
        copyToClipboard.copy(stackFrames.join("\r\n"), "Stack trace was copied to clipboard");
    }
    
    exportAsJson() {
        fileDownloader.downloadAsJson(this.data, "stacks.json");
    }

    fileSelected(fileInput: HTMLInputElement) {
        fileImporter.readAsText(fileInput, data => this.dataImported(data));
    }

    private dataImported(result: string) {
        this.data = JSON.parse(result);
        this.draw();
    }
}

export = captureStackTraces;
