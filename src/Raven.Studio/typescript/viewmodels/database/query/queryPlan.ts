import d3 = require("d3");

type stackFrame = {
    short: string;
    full: string;
}

class executionInfo {
    static boxPadding = 15;
    static lineHeight = 20;
    static headerSize = 50;
    static headerPadding = 3;
    
    x: number;
    y: number;

    children: executionInfo[];
    parent: executionInfo;
    depth: number;
    
    operationName: string;
    parameters: string[];

    constructor(operationName: string, parameters: string[]) {
        this.operationName = operationName;
        this.parameters = parameters;
    }

    boxHeight() {
        return this.parameters.length * executionInfo.lineHeight + 2 * executionInfo.boxPadding;
    }

    paramsWithShortcuts() {
        return this.parameters.map(v => {
            return {
                short: v, //TODO:
                full: v
            } as stackFrame;
        });
    }
    
}

class queryPlan {

    static maxBoxWidth = 280;
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
        const params = Object.entries(node.Parameters).map(x => x[0] + ": " + x[1]); //tODO:
        const info = new executionInfo(node.Operation, params);
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
            .sortKeys(d3.ascending)
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
            .attr("transform", "translate(0,35)");

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

            const stack = parametersContainer
                .selectAll('.parameter')
                .data(d.paramsWithShortcuts());

            stack
                .enter()
                .append('text')
                .attr("class", "parameter")
                .attr('x', -queryPlan.maxBoxWidth / 2 + executionInfo.boxPadding)
                .attr('y', (d, i) => -offsetTop + executionInfo.lineHeight * i)
                .text(d => d.short)
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
            .attr("class", d => "link level-" + (d.source.depth % queryPlan.numberOfDefinedLevels))
            .attr("d", d => lineFunction([
                [d.source.x, d.source.y], 
                [d.source.x, (d.source.y + d.target.y) / 2], 
                [d.target.x, (d.source.y + d.target.y) / 2], 
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
