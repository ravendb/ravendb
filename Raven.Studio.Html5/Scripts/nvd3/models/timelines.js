define(["d3/d3", "d3/nv"], function(d3, nv) {
    nv.models.timelines = function() {
        "use strict";
        //============================================================
        // Public Variables with Default Settings
        //------------------------------------------------------------

        var margin = { top: 0, right: 0, bottom: 0, left: 0 }, width = 960, height = 500, color = nv.utils.defaultColor() // chooses color
            , id = Math.floor(Math.random() * 100000) //Create semi-unique ID incase user doesn't select one
            , x = d3.scale.linear(), y = d3.scale.linear(), getX = function(d) { return d.x } // accessor to get the x value
            , getY = function(d) { return d.y } // accessor to get the y value
            , getSize = function(d) { return d.size || 1 } // accessor to get the point size
            , forceX = [] // List of numbers to Force into the X scale (ie. 0, or a max / min, etc.)
            , forceY = [] // List of numbers to Force into the Y scale
            , forceSize = [] // List of numbers to Force into the Size scale
            , interactive = true // If true, plots a voronoi overlay for advanced point intersection
            , pointKey = null, pointActive = function(d) { return !d.notActive } // any points that return false will be filtered out
            , padData = false // If true, adds half a data points width to front and back, for lining up a line chart with a bar chart
            , padDataOuter = .1 //outerPadding to imitate ordinal scale outer padding
            , clipEdge = false // if true, masks points within x and y scale
            , xDomain = null // Override x domain (skips the calculation from data)
            , yDomain = null // Override y domain
            , xRange = null // Override x range
            , yRange = null // Override y range
            , minBarWidth = 8 // min bar width for easier clicking
            , singlePoint = false, dispatch = d3.dispatch('elementClick', 'elementMouseover', 'elementMouseout');

        //============================================================


        //============================================================
        // Private Variables
        //------------------------------------------------------------

        var x0, y0 // used to store previous scales
            , timeoutID, needsUpdate = false // Flag for when the points are visually updating, but the interactive layer is behind, to disable tooltips
            ;

        //============================================================


        function chart(selection) {
            selection.each(function(data) {
                var availableWidth = width - margin.left - margin.right,
                    availableHeight = height - margin.top - margin.bottom,
                    container = d3.select(this);

                //add series index to each data point for reference
                data.forEach(function(series, i) {
                    series.values.forEach(function(point) {
                        point.series = i;
                    });
                });

                //------------------------------------------------------------
                // Setup Scales

                // remap and flatten the data for use in calculating the scales' domains
                var seriesData = (xDomain && yDomain) ? [] : // if we know xDomain and yDomain, no need to calculate....
                    d3.merge(
                        data.map(function(d) {
                            return d.values.map(function(d, i) {
                                return { x: getX(d, i), y: getY(d, i), size: getSize(d, i) }
                            })
                        })
                    );

                var xMin = -1;
                var xMax = 1;
                if (!xDomain) {
                    xMin = d3.min(seriesData.map(function(d) { return d.x; }));
                    xMax = d3.max(seriesData.map(function(d) { return d.x + d.size; }));
                }

                x.domain(xDomain || [xMin, xMax].concat(forceX));

                if (padData && data[0])
                    x.range(xRange || [(availableWidth * padDataOuter + availableWidth) / (2 * data[0].values.length), availableWidth - availableWidth * (1 + padDataOuter) / (2 * data[0].values.length)]);
                    //x.range([availableWidth * .5 / data[0].values.length, availableWidth * (data[0].values.length - .5)  / data[0].values.length ]);
                else
                    x.range(xRange || [0, availableWidth - minBarWidth - 5]);

                y.domain(yDomain || d3.extent(seriesData.map(function(d) { return d.y; }).concat(forceY)))
                    .range(yRange || [availableHeight, 0]);

                // If scale's domain don't have a range, slightly adjust to make one... so a chart can show a single data point
                if (x.domain()[0] === x.domain()[1] || y.domain()[0] === y.domain()[1]) singlePoint = true;
                if (x.domain()[0] === x.domain()[1])
                    x.domain()[0] ?
                        x.domain([x.domain()[0] - x.domain()[0] * 0.01, x.domain()[1] + x.domain()[1] * 0.01])
                        : x.domain([-1, 1]);

                if (y.domain()[0] === y.domain()[1])
                    y.domain()[0] ?
                        y.domain([y.domain()[0] - y.domain()[0] * 0.01, y.domain()[1] + y.domain()[1] * 0.01])
                        : y.domain([-1, 1]);

                if (isNaN(x.domain()[0])) {
                    x.domain([-1, 1]);
                }

                if (isNaN(y.domain()[0])) {
                    y.domain([-1, 1]);
                }


                x0 = x0 || x;
                y0 = y0 || y;

                //------------------------------------------------------------


                //------------------------------------------------------------
                // Setup containers and skeleton of chart

                var wrap = container.selectAll('g.nv-wrap.nv-scatter').data([data]);
                var wrapEnter = wrap.enter().append('g').attr('class', 'nvd3 nv-wrap nv-scatter nv-chart-' + id + (singlePoint ? ' nv-single-point' : ''));
                var defsEnter = wrapEnter.append('defs');
                var gEnter = wrapEnter.append('g');
                var g = wrap.select('g');

                gEnter.append('g').attr('class', 'nv-groups');
                gEnter.append('g').attr('class', 'nv-point-paths');

                wrap.attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');

                //------------------------------------------------------------


                defsEnter.append('clipPath')
                    .attr('id', 'nv-edge-clip-' + id)
                    .append('rect');

                wrap.select('#nv-edge-clip-' + id + ' rect')
                    .attr('width', availableWidth)
                    .attr('height', (availableHeight > 0) ? availableHeight : 0);

                g.attr('clip-path', clipEdge ? 'url(#nv-edge-clip-' + id + ')' : '');


                function updateInteractiveLayer() {

                    if (!interactive) return false;

                    // add event handlers to points instead voronoi paths
                    wrap.select('.nv-groups').selectAll('.nv-group')
                        .selectAll('.nv-point')
                        .on('click', function(d, i) {
                            if (needsUpdate || !data[d.series]) return 0; //check if this is a dummy point
                            var series = data[d.series],
                                point = series.values[i];

                            dispatch.elementClick({
                                point: point,
                                series: series,
                                pos: [x(getX(point, i)) + margin.left, y(getY(point, i)) + margin.top],
                                seriesIndex: d.series,
                                pointIndex: i
                            });
                        })
                        .on('mouseover', function(d, i) {
                            if (needsUpdate || !data[d.series]) return 0; //check if this is a dummy point
                            var series = data[d.series],
                                point = series.values[i];

                            dispatch.elementMouseover({
                                point: point,
                                series: series,
                                pos: [x(getX(point, i)) + margin.left, y(getY(point, i)) + margin.top],
                                seriesIndex: d.series,
                                pointIndex: i
                            });
                        })
                        .on('mouseout', function(d, i) {
                            if (needsUpdate || !data[d.series]) return 0; //check if this is a dummy point
                            var series = data[d.series],
                                point = series.values[i];

                            dispatch.elementMouseout({
                                point: point,
                                series: series,
                                seriesIndex: d.series,
                                pointIndex: i
                            });
                        });

                    needsUpdate = false;
                }

                needsUpdate = true;

                var groups = wrap.select('.nv-groups').selectAll('.nv-group')
                    .data(function(d) { return d }, function(d) { return d.key });
                groups.enter().append('g')
                    .style('stroke-opacity', 1e-6)
                    .style('fill-opacity', 1e-6);
                groups.exit()
                    .remove();
                groups
                    .attr('class', function(d, i) { return 'nv-group nv-series-' + i })
                    .classed('hover', function(d) { return d.hover });
                groups
                    .transition()
                    .style('fill', function(d, i) { return color(d, i) })
                    .style('stroke', function(d, i) { return color(d, i) })
                    .style('stroke-opacity', 1)
                    .style('fill-opacity', .5);


                var points = groups.selectAll('line.nv-point')
                    .data(function(d) { return d.values }, pointKey);
                points.enter().append('line')
                    .style('fill', function(d, i) { return d.color })
                    .style('stroke', function(d, i) { return d.color })
                    .attr('x1', function(d, i) { return nv.utils.NaNtoZero(x0(getX(d, i))) })
                    .attr('y1', function(d, i) { return nv.utils.NaNtoZero(y0(getY(d, i))) })
                    .attr('x2', function(d, i) { return nv.utils.NaNtoZero(x0(getX(d, i))) })
                    .attr('y2', function(d, i) { return nv.utils.NaNtoZero(y0(getY(d, i))) });
                points.exit().remove();
                groups.exit().selectAll('path.nv-point').remove();
                points.each(function(d, i) {
                    d3.select(this)
                        .classed('nv-point', true)
                        .classed('nv-point-' + i, true)
                        .classed('hover', false);
                });

                points.transition()
                    .attr('x1', function(d, i) { return nv.utils.NaNtoZero(x(getX(d, i))) })
                    .attr('y1', function(d, i) { return nv.utils.NaNtoZero(y(getY(d, i))) })
                    .attr('x2', function(d, i) {
                        var x2 = nv.utils.NaNtoZero(x(getX(d, i) + getSize(d, i)));
                        var x1 = nv.utils.NaNtoZero(x(getX(d, i)));
                        return minBarWidth < (x2 - x1) ? x2 : x1 + minBarWidth;
                    })
                    .attr('y2', function(d, i) { return nv.utils.NaNtoZero(y(getY(d, i))) });

                // Delay updating the invisible interactive layer for smoother animation
                clearTimeout(timeoutID); // stop repeat calls to updateInteractiveLayer
                timeoutID = setTimeout(updateInteractiveLayer, 300);
                //updateInteractiveLayer();

                //store old scales for use in transitions on update
                x0 = x.copy();
                y0 = y.copy();
            });

            return chart;
        }


        //============================================================
        // Event Handling/Dispatching (out of chart's scope)
        //------------------------------------------------------------
        chart.clearHighlights = function() {
            //Remove the 'hover' class from all highlighted points.
            d3.selectAll(".nv-chart-" + id + " .nv-point.hover").classed("hover", false);
        };

        chart.highlightPoint = function(seriesIndex, pointIndex, isHoverOver) {
            d3.select(".nv-chart-" + id + " .nv-series-" + seriesIndex + " .nv-point-" + pointIndex)
                .classed("hover", isHoverOver);
        };

        dispatch.on('elementMouseover.point', function(d) {
            if (interactive) chart.highlightPoint(d.seriesIndex, d.pointIndex, true);
        });

        dispatch.on('elementMouseout.point', function(d) {
            if (interactive) chart.highlightPoint(d.seriesIndex, d.pointIndex, false);
        });

        //============================================================


        //============================================================
        // Expose Public Variables
        //------------------------------------------------------------

        chart.dispatch = dispatch;
        chart.options = nv.utils.optionsFunc.bind(chart);

        chart.x = function(_) {
            if (!arguments.length) return getX;
            getX = d3.functor(_);
            return chart;
        };

        chart.y = function(_) {
            if (!arguments.length) return getY;
            getY = d3.functor(_);
            return chart;
        };

        chart.size = function(_) {
            if (!arguments.length) return getSize;
            getSize = d3.functor(_);
            return chart;
        };

        chart.margin = function(_) {
            if (!arguments.length) return margin;
            margin.top = typeof _.top != 'undefined' ? _.top : margin.top;
            margin.right = typeof _.right != 'undefined' ? _.right : margin.right;
            margin.bottom = typeof _.bottom != 'undefined' ? _.bottom : margin.bottom;
            margin.left = typeof _.left != 'undefined' ? _.left : margin.left;
            return chart;
        };

        chart.width = function(_) {
            if (!arguments.length) return width;
            width = _;
            return chart;
        };

        chart.height = function(_) {
            if (!arguments.length) return height;
            height = _;
            return chart;
        };

        chart.xScale = function(_) {
            if (!arguments.length) return x;
            x = _;
            return chart;
        };

        chart.yScale = function(_) {
            if (!arguments.length) return y;
            y = _;
            return chart;
        };

        chart.xDomain = function(_) {
            if (!arguments.length) return xDomain;
            xDomain = _;
            return chart;
        };

        chart.yDomain = function(_) {
            if (!arguments.length) return yDomain;
            yDomain = _;
            return chart;
        };

        chart.xRange = function(_) {
            if (!arguments.length) return xRange;
            xRange = _;
            return chart;
        };

        chart.yRange = function(_) {
            if (!arguments.length) return yRange;
            yRange = _;
            return chart;
        };

        chart.forceX = function(_) {
            if (!arguments.length) return forceX;
            forceX = _;
            return chart;
        };

        chart.forceY = function(_) {
            if (!arguments.length) return forceY;
            forceY = _;
            return chart;
        };

        chart.forceSize = function(_) {
            if (!arguments.length) return forceSize;
            forceSize = _;
            return chart;
        };

        chart.interactive = function(_) {
            if (!arguments.length) return interactive;
            interactive = _;
            return chart;
        };

        chart.pointKey = function(_) {
            if (!arguments.length) return pointKey;
            pointKey = _;
            return chart;
        };

        chart.pointActive = function(_) {
            if (!arguments.length) return pointActive;
            pointActive = _;
            return chart;
        };

        chart.padData = function(_) {
            if (!arguments.length) return padData;
            padData = _;
            return chart;
        };

        chart.padDataOuter = function(_) {
            if (!arguments.length) return padDataOuter;
            padDataOuter = _;
            return chart;
        };

        chart.clipEdge = function(_) {
            if (!arguments.length) return clipEdge;
            clipEdge = _;
            return chart;
        };

        chart.color = function(_) {
            if (!arguments.length) return color;
            color = nv.utils.getColor(_);
            return chart;
        };

        chart.id = function(_) {
            if (!arguments.length) return id;
            id = _;
            return chart;
        };

        chart.singlePoint = function(_) {
            if (!arguments.length) return singlePoint;
            singlePoint = _;
            return chart;
        };

        chart.minBarWidth = function(_) {
            if (!arguments.length) return minBarWidth;
            minBarWidth = _;
            return chart;
        };

        //============================================================


        return chart;
    };

    return nv;
});
