define(["d3/d3", "d3/nv"], function(d3, nv) {
    nv.models.timelinesChart = function() {
        "use strict";
        //============================================================
        // Public Variables with Default Settings
        //------------------------------------------------------------

        var timelines = nv.models.timelines(), timelines2 = nv.models.timelines(), xAxis = nv.models.axis(), yAxis = nv.models.axis(), x2Axis = nv.models.axis(), y2Axis = nv.models.axis(), legend = nv.models.legend(), controls = nv.models.legend(), distX = nv.models.distribution(), distY = nv.models.distribution(), brush = d3.svg.brush();

        var margin = { top: 30, right: 20, bottom: 30, left: 75 }, margin2 = { top: 0, right: 20, bottom: 20, left: 75 }, width = null, height = null, height2 = 100, color = nv.utils.defaultColor(), x = timelines.xScale(), y = timelines.yScale(), x2 = timelines2.xScale(), y2 = timelines2.yScale(), xPadding = 0, yPadding = 0, showDistX = false, showDistY = false, showLegend = true, brushExtent = null, showXAxis = true, showYAxis = true, rightAlignYAxis = false, showControls = false, tooltips = true, tooltipX = function(key, x, y) { return '<strong>' + x + '</strong>' }, tooltipY = function(key, x, y) { return '<strong>' + y + '</strong>' }, tooltip = null, state = {}, defaultState = null, dispatch = d3.dispatch('tooltipShow', 'tooltipHide', 'stateChange', 'changeState', 'brush', 'controlsChange'), noData = "No Data Available.", transitionDuration = 250;

        timelines
            .xScale(x)
            .yScale(y)
            .clipEdge(true);

        timelines2
            .xScale(x2)
            .yScale(y2).interactive(false);
        xAxis
            .orient('bottom')
            .tickPadding(10);
        yAxis
            .orient((rightAlignYAxis) ? 'right' : 'left')
            .tickPadding(10);
        x2Axis
            .orient('bottom')
            .tickPadding(5);
        y2Axis
            .orient('left');
        distX
            .axis('x');
        distY
            .axis('y');

        controls.updateState(false);

        //============================================================


        //============================================================
        // Private Variables
        //------------------------------------------------------------

        var x0, y0;

        var showTooltip = function(e, offsetElement) {
            //TODO: make tooltip style an option between single or dual on axes (maybe on all charts with axes?)

            var left = e.pos[0] + (offsetElement.offsetLeft || 0),
                top = e.pos[1] + (offsetElement.offsetTop || 0),
                leftX1 = e.pos[0] + (offsetElement.offsetLeft || 0),
                leftX2 = e.x2 + (offsetElement.offsetLeft || 0),
                topX = y.range()[0] + margin.top + (offsetElement.offsetTop || 0),
                leftY = x.range()[0] + margin.left + (offsetElement.offsetLeft || 0),
                topY = e.pos[1] + (offsetElement.offsetTop || 0),
                xVal = xAxis.tickFormat()(timelines.x()(e.point, e.pointIndex)),
                yVal = yAxis.tickFormat()(timelines.y()(e.point, e.pointIndex));

            var shift = (leftX2 - leftX1) / 2;


            if (tooltipX != null)
                nv.tooltip.show([leftX1, topX], tooltipX(e.series.key, xVal, yVal, e, chart), 'n', 1, offsetElement, 'x-nvtooltip');
            if (tooltipY != null)
                nv.tooltip.show([leftY, topY], tooltipY(e.series.key, xVal, yVal, e, chart), 'e', 1, offsetElement, 'y-nvtooltip');
            if (tooltip != null)
                nv.tooltip.show([left + shift, top], tooltip(e.series.key, xVal, yVal, e, chart), e.value < 0 ? 'n' : 's', null, offsetElement);
        };

        var controlsData = [
            { key: 'Allow zooming', disabled: true }
        ];

        //============================================================


        function chart(selection) {
            selection.each(function(data) {
                var container = d3.select(this),
                    that = this;

                var availableWidth = (width || parseInt(container.style('width')) || 960)
                    - margin.left - margin.right,
                    availableHeight1 = (height || parseInt(container.style('height')) || 400)
                        - margin.top - margin.bottom - height2;

                var availableHeight2 = height2 - margin2.top - margin2.bottom;

                chart.update = function() { container.transition().duration(transitionDuration).call(chart); };
                chart.container = this;

                //set state.disabled
                state.disabled = data.map(function(d) { return !!d.disabled });

                if (!defaultState) {
                    var key;
                    defaultState = {};
                    for (key in state) {
                        if (state[key] instanceof Array)
                            defaultState[key] = state[key].slice(0);
                        else
                            defaultState[key] = state[key];
                    }
                }

                //------------------------------------------------------------
                // Display noData message if there's nothing to show.

                if (!data || !data.length || !data.filter(function(d) { return d.values.length }).length) {
                    var noDataText = container.selectAll('.nv-noData').data([noData]);

                    noDataText.enter().append('text')
                        .attr('class', 'nvd3 nv-noData')
                        .attr('dy', '-.7em')
                        .style('text-anchor', 'middle');

                    noDataText
                        .attr('x', margin.left + availableWidth / 2)
                        .attr('y', margin.top + availableHeight1 / 2)
                        .text(function(d) { return d });

                    container.selectAll('.nv-wrap').remove();

                    return chart;
                } else {
                    container.selectAll('.nv-noData').remove();
                }

                //------------------------------------------------------------


                //------------------------------------------------------------
                // Setup Scales

                x0 = x0 || x;
                y0 = y0 || y;

                //------------------------------------------------------------


                //------------------------------------------------------------
                // Setup containers and skeleton of chart

                var wrap = container.selectAll('g.nv-wrap.nv-scatterChart').data([data]);
                var wrapEnter = wrap.enter().append('g').attr('class', 'nvd3 nv-wrap nv-scatterChart nv-chart-' + timelines.id());
                var gEnter = wrapEnter.append('g');
                var g = wrap.select('g');

                var focusEnter = gEnter.append('g').attr('class', 'nv-focus');


                // background for pointer events
                focusEnter.append('rect').attr('class', 'nvd3 nv-background');

                focusEnter.append('g').attr('class', 'nv-x nv-axis');
                focusEnter.append('g').attr('class', 'nv-y nv-axis');
                focusEnter.append('g').attr('class', 'nv-scatterWrap');
                focusEnter.append('g').attr('class', 'nv-distWrap');
                focusEnter.append('g').attr('class', 'nv-legendWrap');
                focusEnter.append('g').attr('class', 'nv-controlsWrap');

                var contextEnter = gEnter.append('g').attr('class', 'nv-context').style('opacity', controlsData[0].disabled ? 0 : 1).style('display', controlsData[0].disabled ? 'none' : 'block')
                contextEnter.append('g').attr('class', 'nv-x nv-axis');
                contextEnter.append('g').attr('class', 'nv-y nv-axis');
                contextEnter.append('g').attr('class', 'nv-scatterWrap');
                contextEnter.append('g').attr('class', 'nv-brushBackground');
                contextEnter.append('g').attr('class', 'nv-x nv-brush');

                //------------------------------------------------------------


                //------------------------------------------------------------
                // Legend

                if (showLegend) {
                    var legendWidth = (showControls) ? availableWidth / 2 : availableWidth;
                    legend.width(legendWidth);

                    wrap.select('.nv-legendWrap')
                        .datum(data)
                        .call(legend);

                    if (margin.top != legend.height()) {
                        margin.top = legend.height();
                        availableHeight1 = (height || parseInt(container.style('height')) || 400)
                            - margin.top - margin.bottom - height2;
                    }

                    wrap.select('.nv-legendWrap')
                        .attr('transform', 'translate(' + (availableWidth - legendWidth) + ',' + (-margin.top) + ')');
                }

                //------------------------------------------------------------


                //------------------------------------------------------------
                // Controls

                if (showControls) {
                    controls.width(180).color(['#444']);
                    g.select('.nv-controlsWrap')
                        .datum(controlsData)
                        .attr('transform', 'translate(0,' + (-margin.top) + ')')
                        .call(controls);
                }

                //------------------------------------------------------------


                wrap.attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');

                if (rightAlignYAxis) {
                    g.select(".nv-y.nv-axis")
                        .attr("transform", "translate(" + availableWidth + ",0)");
                }

                //------------------------------------------------------------
                // Main Chart Component(s)

                timelines
                    .width(availableWidth)
                    .height(availableHeight1)
                    .color(data.map(function(d, i) {
                        return d.color || color(d, i);
                    }).filter(function(d, i) { return !data[i].disabled }));

                timelines2
                    .width(availableWidth)
                    .height(availableHeight2)
                    .interactive(false)
                    .color(data.map(function(d, i) {
                        return d.color || color(d, i);
                    }).filter(function(d, i) { return !data[i].disabled }));

                g.select('.nv-context').attr('transform', 'translate(0,' + (availableHeight1 + margin.bottom + margin2.top) + ')');

                if (xPadding !== 0)
                    timelines.xDomain(null);

                if (yPadding !== 0)
                    timelines.yDomain(null);

                wrap.select('.nv-context .nv-scatterWrap')
                    .datum(data.filter(function(d) { return !d.disabled }))
                    .call(timelines2);

                //------------------------------------------------------------
                // Setup Brush

                brush
                    .x(x2)
                    .on('brush', function() {
                        //When brushing, turn off transitions because chart needs to change immediately.
                        var oldTransition = chart.transitionDuration();
                        chart.transitionDuration(0);
                        onBrush();
                        chart.transitionDuration(oldTransition);
                    });

                if (brushExtent) brush.extent(brushExtent);

                var brushBG = g.select('.nv-brushBackground').selectAll('g')
                    .data([brushExtent || brush.extent()])

                var brushBGenter = brushBG.enter()
                    .append('g');

                brushBGenter.append('rect')
                    .attr('class', 'left')
                    .attr('x', 0)
                    .attr('y', 0)
                    .attr('height', availableHeight2);

                brushBGenter.append('rect')
                    .attr('class', 'right')
                    .attr('x', 0)
                    .attr('y', 0)
                    .attr('height', availableHeight2);

                var gBrush = g.select('.nv-x.nv-brush')
                    .call(brush);
                gBrush.selectAll('rect')
                    //.attr('y', -5)
                    .attr('height', availableHeight2);
                gBrush.selectAll('.resize').append('path').attr('d', resizePath);


                //------------------------------------------------------------

                //Adjust for x and y padding
                if (xPadding !== 0) {
                    var xRange = x.domain()[1] - x.domain()[0];
                    timelines.xDomain([x.domain()[0] - (xPadding * xRange), x.domain()[1] + (xPadding * xRange)]);
                }

                if (yPadding !== 0) {
                    var yRange = y.domain()[1] - y.domain()[0];
                    timelines.yDomain([y.domain()[0] - (yPadding * yRange), y.domain()[1] + (yPadding * yRange)]);
                }

                //Only need to update the timelines again if x/yPadding changed the domain.
                if (yPadding !== 0 || xPadding !== 0) {
                    wrap.select('.nv-scatterWrap')
                        .datum(data.filter(function(d) { return !d.disabled }))
                        .call(timelines);
                }

                //------------------------------------------------------------


                //------------------------------------------------------------
                // Setup Axes
                if (showXAxis) {
                    xAxis
                        .scale(x)
                        .ticks(xAxis.ticks() && xAxis.ticks().length ? xAxis.ticks() : availableWidth / 100)
                        .tickSize(-availableHeight1, 0);

                    g.select('.nv-focus .nv-x.nv-axis')
                        .attr('transform', 'translate(0,' + availableHeight1 + ')')
                        .call(xAxis);

                }

                if (showYAxis) {
                    yAxis
                        .scale(y)
                        .ticks(yAxis.ticks() && yAxis.ticks().length ? yAxis.ticks() : availableHeight1 / 36)
                        .tickSize(-availableWidth, 0);

                    g.select('.nv-focus .nv-y.nv-axis')
                        .call(yAxis);
                }

                // setup brush

                onBrush();

                x2Axis
                    .scale(x2)
                    .ticks(availableWidth / 100)
                    .tickSize(-availableHeight2, 0);

                g.select('.nv-context .nv-x.nv-axis')
                    .attr('transform', 'translate(0,' + y2.range()[0] + ')');
                d3.transition(g.select('.nv-context .nv-x.nv-axis'))
                    .call(x2Axis);


                y2Axis
                    .scale(y2)
                    .ticks(availableHeight2 / 36)
                    .tickSize(-availableWidth, 0);

                d3.transition(g.select('.nv-context .nv-y.nv-axis'))
                    .call(y2Axis);

                g.select('.nv-context .nv-x.nv-axis')
                    .attr('transform', 'translate(0,' + y2.range()[0] + ')');


                if (showDistX) {
                    distX
                        .getData(timelines.x())
                        .scale(x)
                        .width(availableWidth)
                        .color(data.map(function(d, i) {
                            return d.color || color(d, i);
                        }).filter(function(d, i) { return !data[i].disabled }));
                    gEnter.select('.nv-distWrap').append('g')
                        .attr('class', 'nv-distributionX');
                    g.select('.nv-distributionX')
                        .attr('transform', 'translate(0,' + y.range()[0] + ')')
                        .datum(data.filter(function(d) { return !d.disabled }))
                        .call(distX);
                }

                if (showDistY) {
                    distY
                        .getData(timelines.y())
                        .scale(y)
                        .width(availableHeight1)
                        .color(data.map(function(d, i) {
                            return d.color || color(d, i);
                        }).filter(function(d, i) { return !data[i].disabled }));
                    gEnter.select('.nv-focus .nv-distWrap').append('g')
                        .attr('class', 'nv-distributionY');
                    g.select('.nv-focus .nv-distributionY')
                        .attr('transform',
                            'translate(' + (rightAlignYAxis ? availableWidth : -distY.size()) + ',0)')
                        .datum(data.filter(function(d) { return !d.disabled }))
                        .call(distY);
                }


                //============================================================
                // Event Handling/Dispatching (in chart's scope)
                //------------------------------------------------------------

                controls.dispatch.on('legendClick', function(d, i) {
                    dispatch.controlsChange(d, i);
                    d.disabled = !d.disabled;

                    if (d.disabled) {
                        g.select('.nv-context').transition().duration(transitionDuration).style('opacity', 0).each('end', function() {
                            d3.select(this).style('display', 'none');
                        });
                        brushExtent = null;
                        brush.clear();
                    } else {
                        g.select('.nv-context').style('display', 'block').transition().duration(transitionDuration).style('opacity', 1);

                    }
                    chart.update();
                });

                legend.dispatch.on('stateChange', function(newState) {
                    state.disabled = newState.disabled;
                    dispatch.stateChange(state);
                    chart.update();
                });

                timelines.dispatch.on('elementMouseover.tooltip', function(e) {
                    d3.select('.nv-chart-' + timelines.id() + ' .nv-series-' + e.seriesIndex + ' .nv-distx-' + e.pointIndex)
                        .attr('y1', function(d, i) { return e.pos[1] - availableHeight1; });
                    d3.select('.nv-chart-' + timelines.id() + ' .nv-series-' + e.seriesIndex + ' .nv-disty-' + e.pointIndex)
                        .attr('x2', e.pos[0] + distX.size());

                    e.x2 = timelines.xScale()(e.point.x + e.point.size) + margin.left;

                    e.pos = [e.pos[0] + margin.left, e.pos[1] + margin.top];
                    dispatch.tooltipShow(e);
                });

                dispatch.on('tooltipShow', function(e) {
                    if (tooltips) showTooltip(e, that.parentNode);
                });

                // Update chart from a state object passed to event handler
                dispatch.on('changeState', function(e) {

                    if (typeof e.disabled !== 'undefined') {
                        data.forEach(function(series, i) {
                            series.disabled = e.disabled[i];
                        });

                        state.disabled = e.disabled;
                    }

                    chart.update();
                });

                //============================================================


                //store old scales for use in transitions on update
                x0 = x.copy();
                y0 = y.copy();

                // Taken from crossfilter (http://square.github.com/crossfilter/)

                function resizePath(d) {
                    var e = +(d == 'e'),
                        x = e ? 1 : -1,
                        y = availableHeight2 / 3;
                    return 'M' + (.5 * x) + ',' + y
                        + 'A6,6 0 0 ' + e + ' ' + (6.5 * x) + ',' + (y + 6)
                        + 'V' + (2 * y - 6)
                        + 'A6,6 0 0 ' + e + ' ' + (.5 * x) + ',' + (2 * y)
                        + 'Z'
                        + 'M' + (2.5 * x) + ',' + (y + 8)
                        + 'V' + (2 * y - 8)
                        + 'M' + (4.5 * x) + ',' + (y + 8)
                        + 'V' + (2 * y - 8);
                }

                function updateBrushBG() {
                    if (!brush.empty()) brush.extent(brushExtent);
                    brushBG
                        .data([brush.empty() ? x2.domain() : brushExtent])
                        .each(function(d, i) {
                            var leftWidth = x2(d[0]) - x.range()[0],
                                rightWidth = x.range()[1] - x2(d[1]);
                            d3.select(this).select('.left')
                                .attr('width', leftWidth < 0 ? 0 : leftWidth);

                            d3.select(this).select('.right')
                                .attr('x', x2(d[1]))
                                .attr('width', rightWidth < 0 ? 0 : rightWidth);
                        });
                }

                function onBrush() {
                    brushExtent = brush.empty() ? null : brush.extent();
                    var extent = brush.empty() ? x2.domain() : brush.extent();

                    //The brush extent cannot be less than one.  If it is, don't update the line chart.
                    if (Math.abs(extent[0] - extent[1]) <= 1) {
                        return;
                    }

                    dispatch.brush({ extent: extent, brush: brush });

                    updateBrushBG();

                    // Update Main (Focus)
                    timelines.xDomain(extent);
                    g.select('.nv-focus .nv-scatterWrap').datum(data.filter(function(d) { return !d.disabled })).transition().duration(transitionDuration).call(timelines);

                    // Update Main (Focus) Axes
                    g.select('.nv-focus .nv-x.nv-axis').datum(data.filter(function(d) { return !d.disabled })).transition().duration(transitionDuration)
                        .call(xAxis);
                    g.select('.nv-focus .nv-y.nv-axis').datum(data.filter(function(d) { return !d.disabled })).transition().duration(transitionDuration)
                        .call(yAxis);

                    g.select('.nv-focus .nv-distributionX').datum(data.filter(function(d) { return !d.disabled })).transition().duration(transitionDuration)
                        .call(distX);

                    g.select('.nv-focus .nv-distributionY').datum(data.filter(function(d) { return !d.disabled })).transition().duration(transitionDuration)
                        .call(distY);
                }
            });

            return chart;
        }


        //============================================================
        // Event Handling/Dispatching (out of chart's scope)
        //------------------------------------------------------------

        timelines.dispatch.on('elementMouseout.tooltip', function(e) {
            dispatch.tooltipHide(e);

            d3.select('.nv-chart-' + timelines.id() + ' .nv-series-' + e.seriesIndex + ' .nv-distx-' + e.pointIndex)
                .attr('y1', 0);
            d3.select('.nv-chart-' + timelines.id() + ' .nv-series-' + e.seriesIndex + ' .nv-disty-' + e.pointIndex)
                .attr('x2', distY.size());
        });
        dispatch.on('tooltipHide', function() {
            if (tooltips) nv.tooltip.cleanup();
        });

        //============================================================


        //============================================================
        // Expose Public Variables
        //------------------------------------------------------------

        // expose chart's sub-components
        chart.dispatch = dispatch;
        chart.timelines = timelines;
        chart.legend = legend;
        chart.controls = controls;
        chart.xAxis = xAxis;
        chart.yAxis = yAxis;
        chart.x2Axis = x2Axis;
        chart.y2Axis = y2Axis;
        chart.distX = distX;
        chart.distY = distY;

        d3.rebind(chart, timelines, 'id', 'interactive', 'pointActive', 'x', 'y', 'shape', 'size', 'xScale', 'yScale', 'xDomain', 'yDomain', 'xRange', 'yRange', 'sizeDomain', 'sizeRange', 'forceX', 'forceY', 'forceSize', 'clipRadius');
        chart.options = nv.utils.optionsFunc.bind(chart);

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

        chart.color = function(_) {
            if (!arguments.length) return color;
            color = nv.utils.getColor(_);
            legend.color(color);
            distX.color(color);
            distY.color(color);
            return chart;
        };

        chart.showDistX = function(_) {
            if (!arguments.length) return showDistX;
            showDistX = _;
            return chart;
        };

        chart.showDistY = function(_) {
            if (!arguments.length) return showDistY;
            showDistY = _;
            return chart;
        };

        chart.showControls = function(_) {
            if (!arguments.length) return showControls;
            showControls = _;
            return chart;
        };

        chart.showLegend = function(_) {
            if (!arguments.length) return showLegend;
            showLegend = _;
            return chart;
        };

        chart.showXAxis = function(_) {
            if (!arguments.length) return showXAxis;
            showXAxis = _;
            return chart;
        };

        chart.showYAxis = function(_) {
            if (!arguments.length) return showYAxis;
            showYAxis = _;
            return chart;
        };

        chart.rightAlignYAxis = function(_) {
            if (!arguments.length) return rightAlignYAxis;
            rightAlignYAxis = _;
            yAxis.orient((_) ? 'right' : 'left');
            return chart;
        };

        chart.xPadding = function(_) {
            if (!arguments.length) return xPadding;
            xPadding = _;
            return chart;
        };

        chart.yPadding = function(_) {
            if (!arguments.length) return yPadding;
            yPadding = _;
            return chart;
        };

        chart.tooltips = function(_) {
            if (!arguments.length) return tooltips;
            tooltips = _;
            return chart;
        };

        chart.tooltipContent = function(_) {
            if (!arguments.length) return tooltip;
            tooltip = _;
            return chart;
        };

        chart.tooltipXContent = function(_) {
            if (!arguments.length) return tooltipX;
            tooltipX = _;
            return chart;
        };

        chart.tooltipYContent = function(_) {
            if (!arguments.length) return tooltipY;
            tooltipY = _;
            return chart;
        };

        chart.state = function(_) {
            if (!arguments.length) return state;
            state = _;
            return chart;
        };

        chart.defaultState = function(_) {
            if (!arguments.length) return defaultState;
            defaultState = _;
            return chart;
        };

        chart.noData = function(_) {
            if (!arguments.length) return noData;
            noData = _;
            return chart;
        };

        chart.transitionDuration = function(_) {
            if (!arguments.length) return transitionDuration;
            transitionDuration = _;
            return chart;
        };

        chart.brushExtent = function(_) {
            if (!arguments.length) return brushExtent;
            brushExtent = _;
            return chart;
        };

        //============================================================


        return chart;
    }

    return nv;
});
