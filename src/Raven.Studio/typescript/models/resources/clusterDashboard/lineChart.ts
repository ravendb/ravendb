/// <reference path="../../../../typings/tsd.d.ts"/>

import d3 = require("d3");

type chartItemData = {
    x: Date,
    y: number
}

type chartData = {
    id: string,
    values: chartItemData[],
}

type chartOpts = {
    grid?: boolean;
    fillArea?: boolean;
    fillData?: boolean;
    yMaxProvider?: () => number | null;
    useSeparateYScales?: boolean;
    topPaddingProvider?: (key: string) => number;
    tooltipProvider?: (date: Date|null) => string;
    onMouseMove?: (date: Date|null) => void;
}

class lineChart {
    
    static readonly defaultTopPadding = 5;
    static readonly timeFormat = "h:mm:ss A";
    
    private width: number;
    private height: number;
    
    private minDate: Date = null;
    private maxDate: Date = null;
    private data: chartData[] = [];
    private opts: chartOpts;
    
    private svg: d3.Selection<void>;
    private pointer: d3.Selection<void>;
    private lastXPosition: number = null;
    private tooltip: d3.Selection<void>;
    private mouseOver: boolean = false;
    
    private xScale: d3.time.Scale<number, number>;
    
    private containerSelector: string | EventTarget;
    private highlightVisible: boolean = false;
    
    constructor(containerSelector: string | EventTarget, opts?: chartOpts) {
        this.opts = opts || {} as any;
        this.containerSelector = containerSelector;
        
        if (!this.opts.topPaddingProvider) {
            this.opts.topPaddingProvider = () => lineChart.defaultTopPadding;
        }
        
        const container = d3.select(containerSelector as string);
        
        const $container = $(containerSelector);
        
        this.width = $container.innerWidth();
        this.height = $container.innerHeight();

        this.svg = container
            .append("svg")
            .attr("width", this.width)
            .attr("height", this.height);
        
        if (this.opts.grid) {
            const gridContainer = this.svg
                .append("g")
                .attr("transform", "translate(-0.5, 0)")
                .attr("class", "grid");
            this.drawGrid(gridContainer);
        }
        
        this.svg
            .append("g")
            .attr("class", "series");
        
        const pointer = this.svg
            .append("g")
            .attr("class", "pointer");
        
        this.pointer = pointer.append("line")
            .attr("class", "pointer-line")
            .attr("x1", 0)
            .attr("x2", 0)
            .attr("y1", 0)
            .attr("y2", this.height)
            .style("stroke-opacity", 0);
        
        this.tooltip = d3.select(".tooltip");
        
        if (this.opts.tooltipProvider || this.opts.onMouseMove) {
            this.setupValuesPreview();
        }
    }
    
    private drawGrid(gridContainer: d3.Selection<any>) {
        const gridLocation = _.range(0, this.width, 40)
            .map(x => this.width - x);
        
        const lines = gridContainer.selectAll("line")
            .data(gridLocation);
        
        lines
            .exit()
            .remove();
        
        lines
            .enter()
            .append("line")
            .attr("class", "grid-line")
            .attr("x1", x => x)
            .attr("x2", x => x)
            .attr("y1", 0)
            .attr("y2", this.height);
    }
    
    onResize() {
        const container = d3.select(this.containerSelector as string);
        
        const $container = $(this.containerSelector);
        
        this.width = $container.innerWidth();
        this.height = $container.innerHeight();

        this.svg = container
            .select("svg")
            .attr("width", this.width)
            .attr("height", this.height);
        
        //TODO: add viewport? 
        
        const gridContainer = this.svg.select(".grid");
        gridContainer.selectAll("line").remove();
        
        this.drawGrid(gridContainer);
    }
    
    highlightTime(date: Date|null) {
        if (date) {
            const xToHighlight = this.xScale(date);
            
            if (!this.highlightVisible) {
                this.pointer
                    .transition()
                    .duration(200)
                    .style("stroke-opacity", 1);
                this.highlightVisible = true;
            }
            
            this.pointer
                .attr("x1", xToHighlight + 0.5)
                .attr("x2", xToHighlight + 0.5);
        } else {
            this.highlightVisible = false;
            this.pointer
                .transition()
                .duration(100)
                .style("stroke-opacity", 0);
        }
    }
    
    private setupValuesPreview() {
        const withTooltip = !!this.opts.tooltipProvider;
        this.svg
            .on("mousemove.tip", () => {
                if (this.xScale) {
                    const node = this.svg.node();
                    const mouseLocation = d3.mouse(node);

                    const hoverTime = this.xScale.invert(mouseLocation[0]);
                    this.opts?.onMouseMove(hoverTime);

                    if (withTooltip) {
                        this.updateTooltip();
                    }
                }
            })
            .on("mouseenter.tip", () => {
                this.mouseOver = true;
                if (withTooltip) {
                    this.showTooltip();
                }
            })
            .on("mouseleave.tip", () => {
                this.mouseOver = false;
                if (withTooltip) {
                    this.hideTooltip();
                }
                
                this.opts?.onMouseMove(null);
            });
    }
    
    showTooltip() {
        this.tooltip
            .style('display', undefined)
            .transition()
            .duration(250)
            .style("opacity", 1);
    }
    
    updateTooltip(passive = false) {
        let xToUse = null as number;
        if (passive) {
            // just update contents
            xToUse = this.lastXPosition;
        } else {
            xToUse = d3.mouse(this.svg.node())[0];
            this.lastXPosition = xToUse;

            const globalLocation = d3.mouse(d3.select(".cluster-dashboard-container").node());
            const [x, y] = globalLocation;
            this.tooltip
                .style("left", (x + 10) + "px")
                .style("top", (y + 10) + "px")
                .style('display', undefined);
        }
        
        if (!_.isNull(xToUse) && this.minDate) {
            const data = this.findClosestData(xToUse);
            const html = this.opts.tooltipProvider(data) || "";
            
            if (html) {
                this.tooltip.html(html);
                this.tooltip.style("display", undefined);
            } else {
                this.tooltip.style("display", "none");
            }
        }
    }
    
    private findClosestData(xToUse: number): Date {
        const hoverTime = this.xScale.invert(xToUse);
        return hoverTime.getTime() >= this.minDate.getTime() ? hoverTime : null; 
    }
    
    hideTooltip() {
        this.tooltip.transition()
            .duration(250)
            .style("opacity", 0)
            .each('end', () => this.tooltip.style('display', 'none'));

        this.lastXPosition = null;
    }
    
    onData(time: Date, data: { key: string, value: number }[]) {
        if (!this.minDate) {
            this.minDate = time;
        }
        this.maxDate = time;
        
        data.forEach(dataItem => {
            let dataEntry = this.data.find(x => x.id === dataItem.key);
            
            if (!dataEntry) {
                dataEntry = {
                    id: dataItem.key,
                    values: []
                };
                this.data.push(dataEntry);
            }

            dataEntry.values.push({
                x: time,
                y: dataItem.value
            });
        });
        
        this.maybeTrimData();
        
        if (this.lastXPosition && this.opts.onMouseMove) {
            // noinspection JSSuspiciousNameCombination
            const hoverTime = this.xScale.invert(this.lastXPosition);
            this.opts.onMouseMove(hoverTime);
        }
        
        this.updateTooltip(true);
    }
    
    private maybeTrimData() {
        let hasAnyTrim = false;
        for (let i = 0; i < this.data.length; i++) {
            const entry = this.data[i];
            
            if (entry.values.length > 2000) {
                entry.values = entry.values.splice(1500);
                hasAnyTrim = true;
            }
        }
        
        if (hasAnyTrim) {
            this.minDate = _.min(this.data.map(x => x.values[0].x));
        }
    }
    
    private createLineFunctions(): Map<string, d3.svg.Line<chartItemData>> {
        const timePerPixel = 500;
        const maxTime = this.maxDate;
        const minTime = new Date(maxTime.getTime() - this.width * timePerPixel);

        const result = new Map<string, d3.svg.Line<chartItemData>>();

        this.xScale = d3.time.scale()
            .range([0, this.width])
            .domain([minTime, maxTime]);
        
        const yScaleCreator = (maxValue: number, topPadding: number) => {
            if (!maxValue) {
                maxValue = 1;
            }
            return d3.scale.linear()
                .range([topPadding != null ? topPadding : lineChart.defaultTopPadding, this.height])
                .domain([maxValue, 0]);
        };
        
        if (this.opts.yMaxProvider != null) {
            const yScale = yScaleCreator(this.opts.yMaxProvider(), this.opts.topPaddingProvider(null));

            const lineFunction = d3.svg.line<chartItemData>()
                .x(x => this.xScale(x.x))
                .y(x => yScale(x.y));
            
            this.data.forEach(data => {
                result.set(data.id, lineFunction);
            });
        } else if (this.opts.useSeparateYScales) {
            this.data.forEach(data => {
                const yMax = d3.max(data.values.map(x => x.y));
                const yScale = yScaleCreator(yMax, this.opts.topPaddingProvider(data.id));

                const lineFunction = d3.svg.line<chartItemData>()
                    .x(x => this.xScale(x.x))
                    .y(x => yScale(x.y));
                
                result.set(data.id, lineFunction);
            });
        } else {
            const yMax = d3.max(this.data.map(d => d3.max(d.values.map(x => x.y))));
            const yScale = yScaleCreator(yMax, this.opts.topPaddingProvider(null));

            const lineFunction = d3.svg.line<chartItemData>()
                .x(x => this.xScale(x.x))
                .y(x => yScale(x.y));

            this.data.forEach(data => {
                result.set(data.id, lineFunction);
            });
        }
     
        return result;
    }
    
    draw() {
        const series = this.svg
            .select(".series")
            .selectAll(".serie")
            .data(this.data, x => x.id);
        
        series
            .exit()
            .remove();
        
        const enteringSerie = series
            .enter()
            .append("g")
            .attr("class", x => "serie " + x.id);
        
        const lineFunctions = this.createLineFunctions();
        
        enteringSerie
            .append("path")
            .attr("class", "line")
            .attr("d", d => lineFunctions.get(d.id)(this.applyFill(d.values)));

        if (this.opts.fillArea) {
            enteringSerie
                .append("path")
                .attr("class", "fill")
                .attr("d", d => lineFunctions.get(d.id)(lineChart.closedPath(this.applyFill(d.values))));
        }
        
        series
            .select(".line")
            .attr("d", d => lineFunctions.get(d.id)(this.applyFill(d.values)));

        if (this.opts.fillArea) {
            series
                .select(".fill")
                .attr("d", d => lineFunctions.get(d.id)(lineChart.closedPath(this.applyFill(d.values))));
        }
    }
    
    private applyFill(items: chartItemData[]) {
        if (!this.opts.fillData) {
            return items;
        }

        if (!items || items.length === 0) {
            return items;
        }
        
        const lastItem = items[items.length - 1];
        
        // fill up to max value with last seen value
        return [...items, {
            x: this.maxDate,
            y: lastItem.y
        }];
    }
    
    private static closedPath(input: chartItemData[]): chartItemData[] {
        if (input.length === 0) {
            return input;
        }
        
        const firstElement = {
            x: input[0].x,
            y: 0
        } as chartItemData;
        
        const lastElement = {
            x: _.last(input).x,
            y: 0
        } as chartItemData;
        
        return [firstElement].concat(input, [lastElement]);
    } 
}

export = lineChart;
