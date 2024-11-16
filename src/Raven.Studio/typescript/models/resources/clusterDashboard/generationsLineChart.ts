import { chartData, chartItemData, chartItemRange, lineChart } from "models/resources/clusterDashboard/lineChart";
import Update = d3.selection.Update;


export class generationsLineChart<TPayload extends { Date: string }> extends lineChart<TPayload> {

    static readonly normalSize = 2;
    static readonly hoverSize = 4;
    
    protected drawLines(lineFunctions: Map<string, { line : d3.svg.Line<chartItemData>; scale: d3.scale.Linear<number, number> }>, series: Update<chartData>) {
        const lines = series.selectAll(".line")
            .data<chartItemRange>(x => x.ranges);

        lines
            .exit()
            .remove();

        lines.enter()
            .append("path")
            .attr("class", "line");

        lines
            .attr("d", d => {
                const line = lineFunctions.get(d.parent.id).line(this.applyFill(d));
                const points = line.substring(1).split("L");
                let output  = "";
                while (points.length > 0) {
                    const before = points.shift();
                    const after = points.shift();
                    if (before && after) {
                        output += "M" + before + "L" + after;
                    }
                }

                return output;
            });

        const dashes = series.selectAll(".dashes")
            .data<chartItemRange>(x => x.ranges);

        dashes
            .exit()
            .remove();

        dashes.enter()
            .append("path")
            .attr("class", "dashes");

        dashes
            .attr("d", d => lineFunctions.get(d.parent.id).line(this.applyFill(d)));

        const gcEventsGroup = series.selectAll(".gc-event-group")
            .data<chartItemRange>(x => x.ranges);

        gcEventsGroup
            .exit()
            .remove();
        gcEventsGroup.enter()
            .append("g")
            .attr("class", "gc-event-group");

        const gcEvents = gcEventsGroup.selectAll(".gc-event")
            .data(x => x.values.map(v => ({ date: v.x, y: v.y, parentId: x.parent.id })));

        gcEvents
            .exit()
            .remove();
        
        gcEvents
            .enter()
            .append("circle")
            .attr("class", "gc-event")
            .attr("r", generationsLineChart.normalSize);

        gcEvents
            .attr("cx", x => this.xScale(x.date))
            .attr("cy", x => lineFunctions.get(x.parentId).scale(x.y));
    }

    recordNoData() {
        this.data.forEach(dataEntry => {
            if (dataEntry.ranges.length) {
                const lastRange = dataEntry.ranges[dataEntry.ranges.length - 1];
                lastRange.finished = true;
            }
        })
    }
    
    highlightTime(date: ClusterWidgetAlignedDate | null) {

        const showPoint = (date: Date) => {
            this.svg.selectAll(".gc-event")
                .filter(x => Math.abs(x.date.getTime() - date.getTime()) <= 1)
                .transition()
                .duration(200)
                .attr("r", generationsLineChart.hoverSize);
        }
        const hidePoint = (date: Date) => {
            this.svg.selectAll(".gc-event")
                .filter(x => Math.abs(x.date.getTime() - date.getTime()) <= 1) 
                .transition()
                .duration(200)
                .attr("r", generationsLineChart.normalSize);
        }

        if (date) {
            if (this.highlightDate) {
                if (date.getTime() !== this.highlightDate.getTime()) {
                    showPoint(date);
                    hidePoint(this.highlightDate);
                }
            } else {
                showPoint(date);
            }
        } else {
            if (this.highlightDate) {
                hidePoint(this.highlightDate);
            }
        }
        
        super.highlightTime(date);
    }
}
