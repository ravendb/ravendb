/// <reference path="../../../../typings/tsd.d.ts"/>

import d3 = require('d3');

type chartItemData = {
    x: Date,
    y: number
}

type chartData = {
    id: string,
    values: chartItemData[],
}

type chartOpts = {
    yMaxProvider?: () => number | null;
    useSeparateYScales?: boolean;
    topPaddingProvider?: (key: string) => number
}

class dashboardChart {
    
    static readonly defaultTopPadding = 5;
    
    private width: number;
    private height: number;
    
    private maxDate: Date = null;
    private data: chartData[] = [];
    private opts: chartOpts;
    
    private svg: d3.Selection<void>;
    
    constructor(containerSelector: string, opts?: chartOpts) {
        this.opts = opts || {} as any;
        
        if (!this.opts.topPaddingProvider) {
            this.opts.topPaddingProvider = () => dashboardChart.defaultTopPadding;
        }
        const container = d3.select(containerSelector);
        
        const $container = $(containerSelector);
        
        this.width = $container.innerWidth();
        this.height = $container.innerHeight();

        this.svg = container
            .append("svg")
            .attr("width", this.width)
            .attr("height", this.height);
        
        const gridLocation = _.range(0, this.width, 20)
            .map(x => this.width - x);
        
        const gridContainer = this.svg
            .append("g")
            .attr("transform", "translate(-0.5, 0)")
            .attr("class", "grid");
        
        this.svg
            .append("g")
            .attr("class", "series");
        
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
    
    onData(time: Date, data: { key: string,  value: number }[] ) {
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
        
        this.draw();
    }
    
    private createLineFunctions(): Map<string, d3.svg.Line<chartItemData>> {
        const timePerPixel = 500;
        const maxTime = this.maxDate;
        const minTime = new Date(maxTime.getTime() - this.width * timePerPixel);

        const result = new Map<string, d3.svg.Line<chartItemData>>();

        const xScale = d3.time.scale()
            .range([0, this.width])
            .domain([minTime, maxTime]);
        
        const yScaleCreator = (maxValue: number, topPadding: number) => {
            if (!maxValue) {
                maxValue = 1;
            }
            return d3.scale.linear()
                .range([topPadding != null ? topPadding : dashboardChart.defaultTopPadding, this.height])
                .domain([maxValue, 0]);
        };
        
        if (this.opts.yMaxProvider != null) {
            const yScale = yScaleCreator(this.opts.yMaxProvider(), this.opts.topPaddingProvider(null));

            const lineFunction = d3.svg.line<chartItemData>()
                .x(x => xScale(x.x))
                .y(x => yScale(x.y));
            
            this.data.forEach(data => {
                result.set(data.id, lineFunction);
            });
        } else if (this.opts.useSeparateYScales) {
            this.data.forEach(data => {
                const yMax = d3.max(data.values.map(x => x.y));
                const yScale = yScaleCreator(yMax, this.opts.topPaddingProvider(data.id));

                const lineFunction = d3.svg.line<chartItemData>()
                    .x(x => xScale(x.x))
                    .y(x => yScale(x.y));
                
                result.set(data.id, lineFunction);
            });
        } else {
            const yMax = d3.max(this.data.map(d => d3.max(d.values.map(x => x.y))));
            const yScale = yScaleCreator(yMax, this.opts.topPaddingProvider(null));

            const lineFunction = d3.svg.line<chartItemData>()
                .x(x => xScale(x.x))
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
            .attr("d", d => lineFunctions.get(d.id)(d.values));

        enteringSerie
            .append("path")
            .attr("class", "fill")
            .attr("d", d => lineFunctions.get(d.id)(dashboardChart.closedPath(d.values)));
        
        series
            .select(".line")
            .attr("d", d => lineFunctions.get(d.id)(d.values));

        series
            .select(".fill")
            .attr("d", d => lineFunctions.get(d.id)(dashboardChart.closedPath(d.values)));
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

export = dashboardChart;
