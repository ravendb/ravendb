/// <reference path="../../../../typings/tsd.d.ts"/>

import d3 = require("d3");
import moment = require("moment");
import { clusterDashboardChart } from "models/resources/clusterDashboard/clusterDashboardChart";
import Update = d3.selection.Update;
import Line = d3.svg.Line;


export interface chartItemData {
    x: Date;
    y: number;
}

export interface chartData {
    id: string;
    ranges: chartItemRange[];
}

export interface chartItemRange {
    finished: boolean;
    parent: chartData;
    values: chartItemData[];
}

type chartOpts = {
    grid?: boolean;
    fillArea?: boolean;
    fillData?: boolean;
    yMaxProvider?: (data: chartData[]) => number | null;
    useSeparateYScales?: boolean;
    topPaddingProvider?: (key: string) => number;
    tooltipProvider?: (unalignedDate: ClusterWidgetUnalignedDate|null) => string;
    onClick?: () => void;
    onMouseMove?: (date: ClusterWidgetUnalignedDate|null) => void;
}

export class lineChart<TPayload extends { Date: string }> implements clusterDashboardChart<TPayload> {
    
    static readonly defaultTopPadding = 5;
    static readonly timeFormat = "h:mm:ss A";
    
    private width: number;
    private height: number;
    
    private minDate: ClusterWidgetUnalignedDate = null;
    private maxDate: ClusterWidgetUnalignedDate = null;
    private data: chartData[] = [];
    private opts: chartOpts;
    
    protected svg: d3.Selection<void>;
    private pointer: d3.Selection<void>;
    private tooltip: d3.Selection<void>;
    
    protected xScale: d3.time.Scale<number, number>;
    
    private readonly containerSelector: string | EventTarget;
    protected highlightDate: Date;
    private readonly dataProvider: (payload: TPayload) => number;
    
    constructor(containerSelector: string | EventTarget, dataProvider: (payload: TPayload) => number, opts?: chartOpts) {
        this.opts = opts || {} as any;
        this.dataProvider = dataProvider;
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
        
        this.tooltip = d3.select(".cluster-dashboard-tooltip");
        
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
        
        const gridContainer = this.svg.select(".grid");
        gridContainer.selectAll("line").remove();
        
        this.drawGrid(gridContainer);
    }
    
    highlightTime(date: ClusterWidgetAlignedDate|null) {
        if (date) {
            const xToHighlight = Math.round(this.xScale(date));
            if (xToHighlight != null) {
                if (!this.highlightDate) {
                    this.pointer
                        .transition()
                        .duration(200)
                        .style("stroke-opacity", 1);
                }
                this.highlightDate = date;

                this.pointer
                    .attr("x1", xToHighlight - 0.5)
                    .attr("x2", xToHighlight - 0.5);
                
                return;
            }
        } 
        
        // remove highlight - no date or no snap
        this.pointer
            .transition()
            .duration(100)
            .style("stroke-opacity", 0);
        this.highlightDate = null;
    }
    
    private setupValuesPreview() {
        const withTooltip = !!this.opts.tooltipProvider;
        this.svg
            .on("click.tip", () => {
                this.opts?.onClick();
            })
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
                if (withTooltip) {
                    this.showTooltip();
                }
            })
            .on("mouseleave.tip", () => {
                if (withTooltip) {
                    this.hideTooltip();
                }
                
                this.opts?.onMouseMove(null);
            });
    }
    
    showTooltip() {
        this.tooltip
            .style("display", undefined)
            .transition()
            .duration(250)
            .style("opacity", 1);
    }
    
    updateTooltip() {
        const xToUse = d3.mouse(this.svg.node())[0];
        this.tooltip.style('display', undefined);
        
        if (!_.isNull(xToUse) && this.minDate) {
            const date = this.findDate(xToUse);
            const html = this.opts.tooltipProvider(date) || "";
            
            if (html) {
                this.tooltip.html(html);
                this.tooltip.style("display", undefined);
            } else {
                this.tooltip.style("display", "none");
            }
        }
        
        const container = d3.select(".cluster-dashboard-container").node();
        const globalLocation = d3.mouse(container);
        const [x, y] = globalLocation;
        
        const tooltipWidth = $(this.tooltip.node()).width();
        const containerWidth = $(container).innerWidth();
        
        const tooltipX = Math.min(x + 10, containerWidth - tooltipWidth);
        
        this.tooltip
            .style("left", tooltipX + "px")
            .style("top", (y + 10) + "px")
    }
    
    private findDate(xToUse: number): Date {
        // noinspection JSSuspiciousNameCombination
        const hoverTime = this.xScale.invert(xToUse);
        return hoverTime.getTime() >= this.minDate.getTime() ? hoverTime : null; 
    }
    
    hideTooltip() {
        this.tooltip.transition()
            .duration(250)
            .style("opacity", 0)
            .each('end', () => this.tooltip.style('display', 'none'));
    }
    
    onHeartbeat(date: ClusterWidgetUnalignedDate) {
        this.maxDate = date;
    }
    
    onData(key: string, payload: TPayload) {
        const transformed = this.dataProvider(payload);
        if (typeof transformed === "undefined") {
            return;
        }
        const date = moment.utc(payload.Date).toDate();
        this.onDataInternal(date, key, transformed);
        
    }
    private onDataInternal(time: ClusterWidgetUnalignedDate, key: string, value: number) {
        if (!this.minDate) {
            this.minDate = time;
        }
        this.maxDate = time;

        const dataRange = this.getOrCreateRange(key);

        dataRange.values.push({
            x: time,
            y: value
        });
        
        this.maybeTrimData();
    }
    
    private getOrCreateChartData(key: string): chartData {
        let dataEntry = this.data.find(x => x.id === key);

        if (!dataEntry) {
            dataEntry = {
                id: key,
                ranges: []
            };
            this.data.push(dataEntry);
        }
        
        return dataEntry;
    }
    
    private getOrCreateRange(key: string): chartItemRange {
        const dataEntry = this.getOrCreateChartData(key);

        if (dataEntry.ranges.length) {
            const lastRange = dataEntry.ranges[dataEntry.ranges.length - 1];
            if (!lastRange.finished) {
                // reusing last range - it isn't finished yet
                return lastRange;
            }
        }

        const newRange: chartItemRange = {
            values: [],
            parent: dataEntry,
            finished: false
        };

        dataEntry.ranges.push(newRange);
        return newRange;
    }
    
    recordNoData(time: ClusterWidgetUnalignedDate, key: string) {
        const dataEntry = this.data.find(x => x.id === key);
        if (dataEntry?.ranges.length) {
            const lastRange = dataEntry.ranges[dataEntry.ranges.length - 1];
            lastRange.finished = true;
        }
    }
    
    private maybeTrimData() {
        const hasAnyTrim = false;
        
        for (const datum of this.data) {
            const rangesLengths = datum.ranges.map(x => x.values.length);
            if (_.sum(rangesLengths) < 2000) {
                continue;
            }

            let sum = 0;

            for (let i = rangesLengths.length - 1; i >= 0; i--) {
                const currentLength = rangesLengths[i];
                if (sum + currentLength > 1500) {
                    // we have overflow in this chunk over the limit - slice this chunk and all previous
                    const itemsToRemove = 1500 - sum;
                    datum.ranges[i].values = datum.ranges[i].values.slice(itemsToRemove);
                    datum.ranges = datum.ranges.slice(i);
                    break;
                }

                sum += currentLength;
            }
        }

        if (hasAnyTrim) {
            this.minDate = _.min(this.data.filter(x => x.ranges.length).map(d => d.ranges[0].values[0].x));
        }
    }
    
    private createLineFunctions(): Map<string, { line : d3.svg.Line<chartItemData>; scale: d3.scale.Linear<number, number> }> {
        if (!this.data.length) {
            return new Map<string, { line : d3.svg.Line<chartItemData>; scale: d3.scale.Linear<number, number> }>();
        }
        
        const timePerPixel = 500;
        const maxTime = this.maxDate;
        const minTime = new Date(maxTime.getTime() - this.width * timePerPixel);

        const result = new Map<string, { line : d3.svg.Line<chartItemData>; scale: d3.scale.Linear<number, number> }>();

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
            const yScale = yScaleCreator(this.opts.yMaxProvider(this.data), this.opts.topPaddingProvider(null));

            const lineFunction = d3.svg.line<chartItemData>()
                .x(x => this.xScale(x.x))
                .y(x => yScale(x.y));
            
            this.data.forEach(data => {
                result.set(data.id, { line: lineFunction, scale: yScale });
            });
        } else if (this.opts.useSeparateYScales) {
            this.data.forEach(data => {
                const yMax = d3.max(data.ranges.filter(range => range.values.length).map(range => d3.max(range.values.map(values => values.y))));
                const yScale = yScaleCreator(yMax, this.opts.topPaddingProvider(data.id));

                const lineFunction = d3.svg.line<chartItemData>()
                    .x(x => this.xScale(x.x))
                    .y(x => yScale(x.y));
                
                result.set(data.id, { line: lineFunction, scale: yScale });
            });
        } else {
            const yMax = d3.max(this.data.map(data => d3.max(data.ranges.filter(range => range.values.length).map(range => d3.max(range.values.map(values => values.y))))));
            const yScale = yScaleCreator(yMax, this.opts.topPaddingProvider(null));

            const lineFunction = d3.svg.line<chartItemData>()
                .x(x => this.xScale(x.x))
                .y(x => yScale(x.y));

            this.data.forEach(data => {
                result.set(data.id, { line: lineFunction, scale: yScale });
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
        
        series
            .enter()
            .append("g")
            .attr("class", x => "serie " + x.id);
        
        const lineFunctions = this.createLineFunctions();
        
        this.drawLines(lineFunctions, series);
        
        if (this.opts.fillArea) {
            const fills = series.selectAll(".fill")
                .data<chartItemRange>(x => x.ranges);
            
            fills
                .exit()
                .remove();
            
            fills.enter()
                .append("path")
                .classed("fill", true);
            
            fills
                .attr("d", d => lineFunctions.get(d.parent.id).line(lineChart.closedPath(this.applyFill(d))));
        }

        if (this.highlightDate) {
            this.highlightTime(this.highlightDate);
        }
    }
    
    protected drawLines(lineFunctions: Map<string, { line : d3.svg.Line<chartItemData>; scale: d3.scale.Linear<number, number> }>, series: Update<chartData>) {
        const lines = series.selectAll(".line")
            .data<chartItemRange>(x => x.ranges);

        lines
            .exit()
            .remove();

        lines.enter()
            .append("path")
            .attr("class", "line")

        lines
            .attr("d", d => lineFunctions.get(d.parent.id).line(this.applyFill(d)));
    }
    
    protected applyFill(range: chartItemRange) {
        const items = range.values;
        if (!this.opts.fillData || range.finished) {
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
        
        const firstElement: chartItemData = {
            x: input[0].x,
            y: 0
        };
        
        const lastElement: chartItemData = {
            x: _.last(input).x,
            y: 0
        };
        
        return [firstElement].concat(input, [lastElement]);
    } 
}
