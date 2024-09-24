import d3 = require("d3");
import genUtils from "common/generalUtils";
import { parseInt } from "lodash";
import icomoonHelpers from "common/helpers/view/icomoonHelpers";

type parameterFrame = {
    label: string;
    leftPadding?: number;
    html: boolean;
    title: string;
}

type IconWithFixedCodePoint = keyof typeof icomoonHelpers.fixedCodepoints;

const WellKnownParams = {
    IsBoosting: "IsBoosting",
    Count: "Count",
    CountConfidence: "CountConfidence",
    FieldName: "FieldName",
    Ascending: "Ascending",
    BoostFactor: "BoostFactor",
    LowValue: "LowValue",
    HighValue: "HighValue",
    LowOption: "LowOption",
    HighOption: "HighOption",
    IteratorDirection: "IteratorDirection",
    ComparerPrefix: "Comparer"
} as const;

interface Param {
    label: string;
    html?: boolean;
    customTitle?: string;
    leftPadding?: number;
}

class executionInfo {
    static boxPadding = 15;
    static lineHeight = 24;
    static headerSize = 50;
    static headerPadding = 3;
    static parametersIndent = 15;
    
    x: number;
    y: number;

    children: executionInfo[];
    parent: executionInfo;
    depth: number;
    
    operationName: string;
    parameters: Param[] = [];
    direction: Extract<IconWithFixedCodePoint, "corax-forward" | "corax-backward"> = null;
    order: Extract<IconWithFixedCodePoint, "corax-sort-az" | "corax-sort-za"> = null;
    icon: IconWithFixedCodePoint = null;

    constructor(operationName: string, parameters: Record<string, string>) {
        this.operationName = operationName;
        this.icon = executionInfo.iconForOperation(operationName);
        this.processParameters(operationName, parameters);
    }
    
    private static iconForOperation(operationName: string): IconWithFixedCodePoint {
        if (operationName === "SortingMatch") {
            return "corax-sorting-match";
        } else if (operationName === "SortingMultiMatch") {
            return "corax-sorting-match";
        } else if (operationName === "SpatialMatch") {
            return "corax-spatial-match";
        } else if (operationName === "AllEntriesMatch") {
            return "corax-all-entries-match";
        } else if (operationName === "BinaryMatch [And]") {
            return "corax-operator-and";
        } else if (operationName === "BinaryMatch [Or]") {
            return "corax-operator-or";
        } else if (operationName === "BinaryMatch [AndNot]") {
            return "corax-operator-andnot";
        } else if (operationName === "MemoizationMatch [Memoization]") {
            return "corax-memoization-match";
        } else if (operationName === "BoostingMatch") {
            return "corax-boosting-match";
        } else if (operationName.startsWith("IncludeNullMatch")) {
            return "corax-include-null-match";
        } else if (operationName.startsWith("TermMatch ")) {
            return "corax-term-match";
        } else if (operationName.startsWith("MultiTermMatch")) {
            return "corax-multi-term-match";
        } else if (operationName.startsWith("UnaryMatch ")) {
            return "corax-unary-match";
        } else if (operationName.startsWith("MultiUnaryMatch")) {
            return "corax-unary-match";
        } else if (operationName.startsWith("PhraseMatch")) {
            return "corax-phrase-query";
        } else {
            return "corax-fallback"; 
        }
    }
    
    private static maybeFormatNumber(input: string) {
        if (!input) { 
            return null;
        }
        
        // input might be string when matching via terms, ex. Name > "foo"
        if (isNaN(input as any)) {
            return "'" + input + "'";
        }
        
        if (Number.isInteger(parseFloat(input))) {
            return genUtils.formatNumberToStringFixed(parseInt(input, 10), 0);
        } 
        
        return genUtils.formatNumberToStringFixed(parseFloat(input), 4);
    }
    
    private processParameters(operationName: string, parameters: Record<string, string>): void {
        const remainingParams = { ...parameters  };

        if (WellKnownParams.LowValue in remainingParams && WellKnownParams.HighValue in remainingParams) {
            let lowParenthesis = "(";
            let highParenthesis = ")";
            
            const lowValue = remainingParams[WellKnownParams.LowValue];
            const highValue = remainingParams[WellKnownParams.HighValue];
            
            const lowFormatted = lowValue ? executionInfo.maybeFormatNumber(lowValue) : "-∞";
            const highFormatted = highValue ? executionInfo.maybeFormatNumber(highValue) : "+∞";

            if (WellKnownParams.LowOption in remainingParams) {
                if (remainingParams[WellKnownParams.LowOption] === "Inclusive") {
                    lowParenthesis = "⟨";
                }
                delete remainingParams[WellKnownParams.LowOption];
            }

            if (WellKnownParams.HighOption in remainingParams) {
                if (remainingParams[WellKnownParams.HighOption] === "Inclusive") {
                    highParenthesis = "⟩";
                }
                delete remainingParams[WellKnownParams.HighOption];
            }
            
            delete remainingParams[WellKnownParams.LowValue];
            delete remainingParams[WellKnownParams.HighValue];
            
            this.parameters.push({ label: "Range: " + lowParenthesis + lowFormatted + " ; " + highFormatted + highParenthesis });
        }
        
        if (WellKnownParams.Ascending in remainingParams) {
            const asc = remainingParams[WellKnownParams.Ascending];
            delete remainingParams[WellKnownParams.Ascending];
            
            this.order = asc === "True" ? "corax-sort-az" : "corax-sort-za";
        }
        
        if (WellKnownParams.IsBoosting in remainingParams) {
            const boost = remainingParams[WellKnownParams.IsBoosting] === "True";
            delete remainingParams[WellKnownParams.IsBoosting];
            
            let factorPart = "";

            if (WellKnownParams.BoostFactor in remainingParams) {
                const boostFactor = remainingParams[WellKnownParams.BoostFactor];
                delete remainingParams[WellKnownParams.BoostFactor];
                
                factorPart = " (factor: " + boostFactor + ")";
            }
            
            const icon = icomoonHelpers.getCodePointForCanvas(boost ? "check" : "cancel"); 
            const iconColor = boost ? "boost-true" : "boost-false";
            
            this.parameters.push(
                { 
                    label: `<tspan dy="4">Boosting: </tspan>
                            <tspan dy="4" class="icon-style ${iconColor}">${icon}</tspan>
                            <tspan dy="-4">${factorPart}</tspan>`, 
                    html: true, 
                    customTitle: "Boosting: " + (boost ? "true" : "false") + factorPart 
                });
        }

        if (WellKnownParams.Count in remainingParams && WellKnownParams.CountConfidence in remainingParams) {
            const count = remainingParams[WellKnownParams.Count];
            const confidence = remainingParams[WellKnownParams.CountConfidence];

            const countFormatted = parseInt(count, 10).toLocaleString(); 
            
            switch (confidence) {
                case "High":
                    this.parameters.push({ label: "Count: " + countFormatted });
                    break;
                case "Normal":
                    this.parameters.push({ label: "Count: ~" + countFormatted });
                    break;
                case "Low":
                    this.parameters.push({ label: "Count: <unknown>" });
                    break;
            }

            delete remainingParams[WellKnownParams.CountConfidence];
            delete remainingParams[WellKnownParams.Count];
        }

        if (WellKnownParams.FieldName in remainingParams) {
            const field = remainingParams[WellKnownParams.FieldName];
            if (field) {
                const fieldTokens = field.split("|").map(x => x.trim());
                if (fieldTokens.length > 0) {
                    this.parameters.push({ label: "Field:" });
                    this.parameters.push(...fieldTokens.map(x => ({ label: x, leftPadding: executionInfo.parametersIndent })));
                }
            }
            
            delete remainingParams[WellKnownParams.FieldName];
        }

        if (WellKnownParams.IteratorDirection in remainingParams) {
            const direction = remainingParams[WellKnownParams.IteratorDirection];
            if (direction) {
                this.direction = executionInfo.mapDirection(direction);

                delete remainingParams[WellKnownParams.IteratorDirection];
            }
        }
        
        if (operationName === "SortingMultiMatch") {
            // try to extract keys which starts with Comparer and create rich component
            const keysToExtract = Object.keys(remainingParams).filter(x => x.startsWith(WellKnownParams.ComparerPrefix));
            const parsed: Array<{ ascending: boolean; fieldName: string; fieldType: string; }> = [];
            
            keysToExtract.forEach(key => {
                const value = remainingParams[key];
                const keyWoPrefix = key.substring(WellKnownParams.ComparerPrefix.length);
                const [indexStr, keySuffix] = keyWoPrefix.split("_", 2);
                const index = parseInt(indexStr, 10);
                
                parsed[index] ??= {
                    ascending: null,
                    fieldName: null,
                    fieldType: null,
                };
                
                const targetObject = parsed[index];

                switch (keySuffix) {
                    case "FieldName":
                        targetObject.fieldName = value;
                        break;
                    case "Ascending":
                        targetObject.ascending = value === "True";
                        break;
                    case "FieldType":
                        targetObject.fieldType = value;
                        break;
                    default:
                        console.error("Unrecognized field: " + key + " (value = " + value + ")");
                        return;
                }
                
                delete remainingParams[key];
            });

            if (parsed.length > 0) {
                parsed.map((order, idx) => {
                    const icon = icomoonHelpers.getCodePointForCanvas(order.ascending ? "corax-sort-az" : "corax-sort-za");
                    this.parameters.push({
                        label: `<tspan dy="4">Comparer #${idx}: </tspan><tspan dy="4" class="icon-style order-icon"> ${icon}</tspan>`,
                        html: true,
                        customTitle: order.ascending ? "Order: Ascending" : "Order: Descending"
                    });
                    this.parameters.push({
                        label: "Field: ",
                        leftPadding: executionInfo.parametersIndent
                    });
                    this.parameters.push({
                        label: order.fieldName,
                        leftPadding: executionInfo.parametersIndent * 2,
                    });
                    this.parameters.push({
                        label: "Field Type: " + order.fieldType,
                        leftPadding: executionInfo.parametersIndent
                    });
                })
            }
        }

        this.parameters.push(...Object.entries(remainingParams ?? []).map(x => ({ label: x[0] + ": " + x[1] })));
    }
    
    private static mapDirection(source: string): "corax-forward" | "corax-backward" {
        if (!source) {
            return null;
        }
        
        switch (source) {
            case "Forward":
                return "corax-forward";
            case "Backward":
                return "corax-backward";
            default:
                return null;
        }
    }
    
    labelForDirection() {
        switch (this.direction) {
            case "corax-backward": 
                return "Iterator Direction: backward";
            case "corax-forward":
                return "Iterator Direction: forward";
            default: 
                return null;
        }
    }

    labelForOrder() {
        switch (this.order) {
            case "corax-sort-az":
                return "Order: ascending";
            case "corax-sort-za":
                return "Order: descending";
            default:
                return null;
        }
    }

    boxHeight() {
        return this.parameters.length * executionInfo.lineHeight + 2 * executionInfo.boxPadding;
    }

    paramsWithShortcuts() {
        return this.parameters.map((v): parameterFrame  => {
            return {
                label: v.label,
                leftPadding: v.leftPadding,
                title: v.customTitle ?? v.label,
                html: v.html ?? false
            };
        });
    }
    
}

class queryPlan {

    static maxBoxWidth = 380;
    static boxVerticalPadding = 100;
    static containerSelector = "#js-query-plan-container";
    static numberOfDefinedLevels = 8;
    
    get container() {
        return $(queryPlan.containerSelector);
    }
    
    clearGraph() {
        const $container = this.container;
        $container.empty();
    }
    
    draw(node: Raven.Client.Documents.Queries.Timings.QueryInspectionNode) {
        this.clearGraph();
        
        const root = queryPlan.toGraph(node);
        this.updateGraph(root);
    }
    
    private static toGraph(node: Raven.Client.Documents.Queries.Timings.QueryInspectionNode): executionInfo {
        const info = new executionInfo(node.Operation, node.Parameters);
        info.children = node.Children ? node.Children.map(queryPlan.toGraph) : [];
        return info;
    }

    private updateGraph(root: executionInfo) {
        this.clearGraph();

        const $container = $(queryPlan.containerSelector);

        const svg = d3.select(queryPlan.containerSelector)
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

        innerGroup.append("rect")
            .attr("class", "overlay");

        const nodeSelector = innerGroup.selectAll(".node");
        const linkSelector = innerGroup.selectAll(".link");

        const treeLayout = d3.layout.tree<executionInfo>()
            .nodeSize([queryPlan.maxBoxWidth + 20, 100])
            .separation((a, b) => a.parent == b.parent ? 1 : 1.15);
        const nodes = treeLayout.nodes(root);

        const maxBoxHeightOnDepth = d3.nest<executionInfo>()
            .key(d => d.depth.toString())
            .sortKeys((a, b) => parseInt(a, 10) - parseInt(b, 10))
            .rollup((leaves: any[]) => d3.max(leaves, l => l.boxHeight()))
            .entries(nodes)
            .map(v => v.values);
        
        const cumulativeHeight = queryPlan.cumulativeSumWithPadding(maxBoxHeightOnDepth, queryPlan.boxVerticalPadding);
        
        const totalHeight = cumulativeHeight[cumulativeHeight.length - 1];
        const xExtent = d3.extent(nodes, (node: any) => node.x);
        xExtent[1] += queryPlan.maxBoxWidth;
        const totalWidth = xExtent[1] - xExtent[0];

        const scale = Math.min(1, Math.max(0.1, 0.96 * Math.min($container.innerWidth() / totalWidth, $container.innerHeight() / totalHeight)));
        
        const zoom = d3.behavior.zoom()
            .scale(scale)
            .scaleExtent([0.1, 1.5])
            .on("zoom", () => this.zoom(innerGroup));

        mainGroup.call(zoom);

        zoom.event(mainGroup);

        d3.select(".overlay")
            .attr("x", -totalWidth * 4)
            .attr("y", -totalHeight * 4)
            .attr("width", totalWidth * 8)
            .attr("height", totalHeight * 8);

        const halfBoxShift = queryPlan.maxBoxWidth / 2 + 10; // little padding

        const xScale = d3.scale.linear()
            .domain([xExtent[0] - halfBoxShift, xExtent[1] - halfBoxShift])
            .range([0, totalWidth]);

        const yScale = d3.scale.linear()
            .domain([0, totalHeight])
            .range([0, totalHeight]);

        const yDepthScale = d3.scale.linear().domain(d3.range(0, cumulativeHeight.length + 1, 1)).range(cumulativeHeight);
        
        const links = treeLayout.links(nodes)
            .map(link => {
                const sourceY = yDepthScale(link.source.depth) + link.source.boxHeight() + executionInfo.headerSize;
                const targetY = yDepthScale(link.target.depth);
                
                return {
                    source: {
                        x: link.source.x,
                        y: sourceY,
                        depth: link.source.depth
                    },
                    target: {
                        x: link.target.x,
                        y: targetY,
                    }
                }
            });

        const nodeSelectorWithData = nodeSelector.data(nodes);
        const linkSelectorWithData = linkSelector.data(links);

        const enteringNodes = nodeSelectorWithData
            .enter()
            .append("g")
            .attr("class", "nodeGroup")
            .style("opacity", 0)
            .attr("transform", d => "translate(" + xScale(d.x) + "," + yScale(yDepthScale(d.depth)) + ")");
        
        enteringNodes
            .transition()
            .delay(d => (d.depth - 1) * 100 + 300)
            .duration(300)
            .style("opacity", 1);
        
        enteringNodes.append('rect')
            .attr('class', 'nodeContainer')
            .attr('x', -queryPlan.maxBoxWidth / 2)
            .attr('y', 0)
            .attr('width', queryPlan.maxBoxWidth)
            .attr('height', d => d.boxHeight() + executionInfo.headerSize);

        const clipPaths = svgDefs
            .selectAll(".parametersClip")
            .data(nodes);

        clipPaths
            .enter()
            .append("clipPath")
            .attr('class', "parametersClip")
            .attr('id', (d, i) => 'params-clip-path-' + i)
            .append('rect')
            .attr('x', -queryPlan.maxBoxWidth / 2)
            .attr('width', queryPlan.maxBoxWidth - executionInfo.boxPadding)
            .attr('y', 0)
            .attr('height', d => d.boxHeight() + executionInfo.headerSize);

        enteringNodes
            .append("rect")
            .attr("class", "header")
            .attr("x", -1 * queryPlan.maxBoxWidth / 2)
            .attr("width", queryPlan.maxBoxWidth)
            .attr("y", 0)
            .attr("height", executionInfo.headerSize);

        const headerGroup = enteringNodes
            .append("g")
            .attr("transform", "translate(0,32)");

        headerGroup
            .append("text")
            .attr("class", "title")
            .attr('text-anchor', "middle")
            .attr("x", 3)
            .text(d => d.operationName);
        
        enteringNodes.each(function (d: executionInfo, index: number) {
            const offsetTop = -executionInfo.headerSize - executionInfo.boxPadding - executionInfo.lineHeight/2;
            const parametersContainer = d3.select(this)
                .append("g")
                .attr('class', 'parameters') 
                .style('clip-path', () => 'url(#params-clip-path-' + index + ')'); 

            const parameters = parametersContainer
                .selectAll('.parameter')
                .data(d.paramsWithShortcuts());

            const enteringParameters= parameters
                .enter()
                .append("text")
                .attr("class", "parameter")
                .attr('x', d => -queryPlan.maxBoxWidth / 2 + executionInfo.boxPadding + (d.leftPadding ?? 0))
                .attr('y', (d, i) => -offsetTop + executionInfo.lineHeight * i);

            enteringParameters
                .filter(x => x.html)
                .html(x => x.label);

            enteringParameters
                .filter(x => !x.html)
                .text(x => x.label);
            
            enteringParameters
                .append("title")
                .text(d => d.title);
        });

        enteringNodes
            .filter(x => !!x.icon)
            .append("text")
            .attr('x', -queryPlan.maxBoxWidth / 2 + 10)
            .attr("y", executionInfo.headerSize / 2)
            .attr("class", "icon-style execution-type-icon")
            .attr("title", x => "Operation Type: " + x.operationName)
            .html(x => icomoonHelpers.getCodePointForCanvas(x.icon));

        enteringNodes
            .filter(x => !!x.direction)
            .append("text")
            .attr('x', queryPlan.maxBoxWidth / 2 - 10)
            .attr("y", executionInfo.headerSize / 2)
            .attr("text-anchor", "end")
            .attr("class", "icon-style direction-type-icon")
            .attr("title", x => x.labelForDirection())
            .html(x => icomoonHelpers.getCodePointForCanvas(x.direction));

        enteringNodes
            .filter(x => !!x.order)
            .append("text")
            .attr('x', queryPlan.maxBoxWidth / 2 - 10)
            .attr("y", executionInfo.headerSize / 2)
            .attr("text-anchor", "end")
            .attr("class", "icon-style order-type-icon")
            .attr("title", x => x.labelForOrder())
            .html(x => icomoonHelpers.getCodePointForCanvas(x.order));
        
        const lineFunction = d3.svg.line()
            .x(x => xScale(x[0]))
            .y(y => yScale(y[1]))
            .interpolate("linear");

        linkSelectorWithData
            .enter()
            .append("path")
            .attr("class", d => "link level-" + (d.source.depth % queryPlan.numberOfDefinedLevels))
            .attr("d", d => lineFunction([
                [d.source.x, d.source.y], 
                [d.source.x, d.target.y - 25], 
                [d.target.x, d.target.y - 25], 
                [d.target.x, d.target.y]
            ]))
            .style("opacity", 0)
            .transition()
            .delay(d => (d.source.depth - 1) * 100 + 300)
            .duration(300)
            .style("opacity", 1);
    }

    zoom(container: d3.Selection<void>) {
        const event = d3.event as d3.ZoomEvent;
        container.attr("transform", "translate(" + event.translate + ")scale(" + event.scale + ")");
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

}

export = queryPlan;
