import { useCallback, useEffect, useRef } from "react";
import { select, Selection, pointer } from "d3-selection";
import { HierarchyNode, HierarchyRectangularNode, treemap } from "d3-hierarchy";
import "d3-transition";
import {
    createHierarchyWithValues,
    hasChildren,
    StorageReportItem,
} from "components/pages/resources/manageServer/storageReport/partials/common";
import genUtils from "common/generalUtils";

interface StorageReportGraphProps {
    node: StorageReportItem;
    rootHierarchy: HierarchyNode<StorageReportItem>;
    onNodeSelected: (node: StorageReportItem) => void;
}

export function StorageReportGraph(props: StorageReportGraphProps) {
    const { node, rootHierarchy, onNodeSelected } = props;

    const chartRef = useRef<HTMLDivElement>();
    const tooltipRef = useRef<HTMLDivElement>();
    const graphDimensions = useRef<{ width: number; height: number }>(null);
    const transitioning = useRef<boolean>(false);
    const previousNode = useRef<StorageReportItem>();
    const previousTreeMap = useRef<HierarchyRectangularNode<StorageReportItem>>();

    const draw = useCallback(
        async (node: StorageReportItem, rootHierarchy: HierarchyNode<StorageReportItem>) => {
            const levelDown = previousNode.current && previousNode.current.internalChildren.includes(node);
            const previousAncestors = previousNode.current
                ? rootHierarchy.find((x) => x.data === previousNode.current).ancestors()
                : [];
            const levelUp = previousNode.current && !!previousAncestors.find((x) => x.data === node);
            const svg = select(chartRef.current).select("svg");

            function drawNewTreeMap(
                nodes: HierarchyRectangularNode<StorageReportItem>[],
                container: Selection<any, any, HTMLElement, any>
            ) {
                const showTypeOffset = 7;
                const showTypePredicate = (d: HierarchyRectangularNode<StorageReportItem>) =>
                    d.data.showType && d.y1 - d.y0 > 22 && d.x1 - d.x0 > 20;

                const cell = container
                    .selectAll("g.cell-no-such") // we always select non-existing nodes to draw from scratch - we don't update elements here
                    .data(nodes)
                    .enter()
                    .append("svg:g")
                    .attr("class", (d) => "cell " + d.data.type)
                    .attr("transform", (d) => "translate(" + d.x0 + "," + d.y0 + ")")
                    .on("click", (event: PointerEvent, d) => {
                        event.preventDefault();
                        if (!transitioning.current) {
                            onNodeSelected(d.data);
                        }
                    })
                    .on("mouseover", (event, data) => onMouseOver(event, data.data))
                    .on("mouseout", () => onMouseOut())
                    .on("mousemove", (e) => onMouseMove(e));

                const rectangles = cell
                    .append("svg:rect")
                    .attr("width", (d) => Math.max(0, d.x1 - d.x0 - 1))
                    .attr("height", (d) => Math.max(0, d.y1 - d.y0 - 1));

                rectangles.filter((x) => hasChildren(x.data)).style("cursor", "pointer");

                cell.append("svg:text")
                    .filter((d) => d.x1 - d.x0 > 20 && d.y1 - d.y0 > 8)
                    .attr("x", (d) => (d.x1 - d.x0) / 2)
                    .attr("y", (d) => (showTypePredicate(d) ? (d.y1 - d.y0) / 2 - showTypeOffset : (d.y1 - d.y0) / 2))
                    .attr("dy", ".35em")
                    .attr("text-anchor", "middle")
                    .text((d) => d.data.name)
                    .each(function (d) {
                        wrap(this, d.x1 - d.x0);
                    });

                cell.filter((d) => showTypePredicate(d))
                    .append("svg:text")
                    .attr("x", (d) => (d.x1 - d.x0) / 2)
                    .attr("y", (d) => (showTypePredicate(d) ? (d.y1 - d.y0) / 2 + showTypeOffset : (d.y1 - d.y0) / 2))
                    .attr("dy", ".35em")
                    .attr("text-anchor", "middle")
                    .text((d) => _.upperFirst(d.data.type))
                    .each(function (d) {
                        wrap(this, d.x1 - d.x0);
                    });

                return cell;
            }

            async function animateZoomIn(
                nodes: HierarchyRectangularNode<StorageReportItem>[],
                oldNode: HierarchyRectangularNode<StorageReportItem>
            ) {
                const oldLocation = oldNode
                    ? {
                          dx: oldNode.x1 - oldNode.x0,
                          dy: oldNode.y1 - oldNode.y0,
                          x: oldNode.x0,
                          y: oldNode.y0,
                      }
                    : {
                          dx: graphDimensions.current.width,
                          dy: graphDimensions.current.height,
                          x: 0,
                          y: 0,
                      };

                const oldContainer = svg.select(".treemap");
                const newGroup = svg.append("g").classed("treemap", true);

                const scaleX = graphDimensions.current.width / oldLocation.dx;
                const scaleY = graphDimensions.current.height / oldLocation.dy;
                const transX = -oldLocation.x * scaleX;
                const transY = -oldLocation.y * scaleY;

                oldContainer
                    .selectAll("text")
                    .transition()
                    .duration(animationLength / 4)
                    .style("opacity", 0);

                await oldContainer
                    .transition()
                    .duration(animationLength)
                    .attr("transform", "translate(" + transX + "," + transY + ")scale(" + scaleX + "," + scaleY + ")")
                    .end();

                const newCells = drawNewTreeMap(nodes, newGroup);
                await newCells.style("opacity", 0).transition().duration(animationLength).style("opacity", 1).end();

                oldContainer.remove();
            }

            async function animateZoomOut(nodes: HierarchyRectangularNode<StorageReportItem>[]) {
                const oldContainer = svg.select(".treemap");
                const newGroup = svg.append("g").classed("treemap", true);
                const newCells = drawNewTreeMap(nodes, newGroup);

                await newCells.style("opacity", 0).transition().duration(animationLength).style("opacity", 1).end();

                oldContainer.remove();
            }

            function onMouseOver(event: React.MouseEvent, d: StorageReportItem) {
                const tooltipSelection = select(tooltipRef.current);
                tooltipSelection.transition().duration(200).style("opacity", 1);

                let html = "<span class='name'>Name: " + d.name + "</span>";
                if (d.showType) {
                    html += "<span>Type: <strong>" + _.upperFirst(d.type) + "</strong></span>";
                }
                if (shouldDisplayNumberOfEntries(d)) {
                    html += "<span>Entries: <strong>" + d.numberOfEntries.toLocaleString() + "</strong></span>";
                }
                html += "<span class='size'>Size: <strong>" + genUtils.formatBytesToSize(d.size) + "</strong></span>";

                tooltipSelection.html(html);
                onMouseMove(event);
            }

            function onMouseMove(e: React.MouseEvent) {
                // eslint-disable-next-line prefer-const
                let [x, y] = pointer(e, chartRef.current);

                const tooltipWidth = tooltipRef.current.getBoundingClientRect().width + 20;

                x = Math.min(x, Math.max(graphDimensions.current.width - tooltipWidth, 0));

                const tooltipSelection = select(tooltipRef.current);
                tooltipSelection.style("left", x + 10 + "px").style("top", y + 10 + "px");
            }

            function onMouseOut() {
                const tooltipSelection = select(tooltipRef.current);
                tooltipSelection.transition().duration(500).style("opacity", 0);
            }

            const oldNode = previousTreeMap.current?.find((x) => x.data === node);

            // construct fake hierarchy based on current node and it's children only - as we draw one level at the time
            const flatHierarchy = createHierarchyWithValues(node, true);
            const currentRoot = treemap<StorageReportItem>().size([
                graphDimensions.current.width,
                graphDimensions.current.height,
            ])(flatHierarchy);

            previousNode.current = node;
            previousTreeMap.current = currentRoot;

            const nodes = currentRoot.children;

            transitioning.current = true;
            try {
                if (levelDown) {
                    await animateZoomIn(nodes, oldNode);
                } else if (levelUp) {
                    await animateZoomOut(nodes);
                } else {
                    // initial state
                    svg.select(".treemap").remove();
                    const container = svg.append("g").classed("treemap", true);
                    drawNewTreeMap(nodes, container);
                }
            } finally {
                transitioning.current = false;
            }

            $(tooltipRef.current).tooltip();
        },
        [onNodeSelected]
    );

    useEffect(() => {
        const boundingRect = chartRef.current.getBoundingClientRect();
        if (!boundingRect) {
            throw new Error("Unable to find graph in screen.");
        }
        graphDimensions.current = { width: boundingRect.width, height: boundingRect.height };

        select(chartRef.current)
            .append("svg:svg")
            .attr("width", graphDimensions.current.width)
            .attr("height", graphDimensions.current.height)
            .attr("transform", "translate(.5,.5)");
    }, []);

    useEffect(() => {
        draw(node, rootHierarchy);
    }, [node, draw, rootHierarchy]);

    return (
        <div id="storage-report-container">
            <div className="chart-tooltip" ref={tooltipRef} style={{ opacity: 0 }}></div>
            <div className="chart" ref={chartRef} data-testid="chart"></div>
        </div>
    );
}

const animationLength = 200;

function wrap($self: any, width: number) {
    const self = select<SVGTextElement, HierarchyRectangularNode<StorageReportItem>>($self);
    let textLength = self.node().getComputedTextLength();
    let text = self.text();
    while (textLength > width - 6 && text.length > 0) {
        text = text.slice(0, -1);
        self.text(text + "...");
        textLength = self.node().getComputedTextLength();
    }
}

function shouldDisplayNumberOfEntries(node: StorageReportItem) {
    return node.type === "tree" || node.type === "table";
}
