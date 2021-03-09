/// <reference path="../../../../typings/tsd.d.ts" />

import colorsManager = require("common/colorsManager");

class graphHelper {

    private static readonly scrollWidth = 8;
    
    static prefixStyle(value: string) {
        const prefix = "-webkit-transform" in document.body.style ? "-webkit-"
            : "-moz-transform" in document.body.style ? "-moz-"
                : "";

        return prefix + value;
    }
    
    static quadraticBezierCurve(start: { x: number, y: number }, end: {x: number, y: number }, delta: number, shorten: number = 0) {
        let x1 = start.x;
        let y1 = start.y;
        let x2 = end.x;
        let y2 = end.y;
        
        if (shorten) {
            ({x1, x2, y1, y2} = graphHelper.shortenLine(x1, y1, x2, y2, shorten));
        }
        
        const pX = (x1 + x2) / 2;
        const pY = (y1 + y2) / 2;
        const sign = Math.sign(y1 - y2);
        
        const coeff = sign ?  (-x2 + x1) / (y2 - y1) : (-x2 + x1) / 1e-6;
        const alpha = Math.atan(coeff);
        
        return `M ${x1} ${y1} Q ${pX + sign * delta * Math.cos(alpha)} ${pY + sign * delta * Math.sin(alpha)} ${x2} ${y2}`;
    }

    static truncText(input: string, measuredWidth: number, availableWidth: number, minWidth = 5): string {
        if (availableWidth >= measuredWidth) {
            return input;
        }
        if (availableWidth < minWidth) {
            return null;
        }

        const approxCharactersToTake = Math.floor(availableWidth * input.length / measuredWidth);
        return input.substr(0, approxCharactersToTake);
    }

    static trimText(input: string, lengthProvider: (numberOfCharacters: number) => number, minWidth: number, maxWidth: number, extraPadding: number = 0): { text: string, containerWidth: number } {
        if (!input) {
            return {
                containerWidth: minWidth,
                text: ""
            }
        }

        const totalWidth = lengthProvider(input.length);
        if (totalWidth + extraPadding < minWidth) {
            return {
                containerWidth: minWidth,
                text: input
            };
        }

        if (totalWidth + extraPadding < maxWidth) {
            return {
                containerWidth: totalWidth + extraPadding,
                text: input
            };
        }

        // text is too long - we have to trim

        const ellipsisWidthApprox = lengthProvider(3);
        let lengthToFit = input.length;

        while (lengthToFit > 0) {
            lengthToFit--;

            const trimmedLength = lengthProvider(lengthToFit);
            const containerWidth = trimmedLength + ellipsisWidthApprox + extraPadding;
            if (containerWidth < maxWidth) {
                return {
                    text: input.substring(0, lengthToFit) + "...",
                    containerWidth: containerWidth
                }
            }
        }

        // fallback
        return {
            containerWidth: minWidth,
            text: ""
        };
    }
    
    static readScrollConfig(): scrollColorConfig {
        const config = {
            scrollColor: undefined as string,
            trackColor: undefined as string
        } as scrollColorConfig;
        
        colorsManager.setup(".graph-helper", config);
        
        return config;
    }
    
    static drawErrorLine(ctx: CanvasRenderingContext2D, x: number, y: number, dy: number) {
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(x, y);
        ctx.lineTo(x, y + dy);
        ctx.stroke();
    }
    
    static drawErrorMark(ctx: CanvasRenderingContext2D, x: number, y: number, dx: number, dy: number = 8) {
        const markWidth = dy;
        if (dx > markWidth) {
            // draw full triangle
            ctx.beginPath();
            ctx.moveTo(x + dx, y);
            ctx.lineTo(x + dx - markWidth, y);
            ctx.lineTo(x + dx, y + markWidth);
            ctx.fill();
        } else if (dx > 1) {
            ctx.beginPath();
            ctx.moveTo(x + dx, y);
            ctx.lineTo(x, y);
            ctx.lineTo(x, y + markWidth - dx);
            ctx.lineTo(x + dx, y + markWidth);
            ctx.fill();
        }
    }
    
    static drawPendingArea(ctx: CanvasRenderingContext2D, x: number, y: number, dx: number) {
        ctx.setLineDash([6, 2]);
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(x, y);
        ctx.lineTo(x + dx, y);
        ctx.stroke();
    }
    
    static drawScroll(ctx: CanvasRenderingContext2D, scrollLocation: { left: number, top: number }, topScrollOffset: number, visibleHeight: number, 
                      totalHeight: number, colors: scrollColorConfig) {
        if (!colors) {
            throw new Error("Missing color config.");
        }
        
        if (visibleHeight > totalHeight) {
            // don't draw scrollbar
            return;
        }
        ctx.save();
        ctx.translate(scrollLocation.left, scrollLocation.top);

        try {
            ctx.fillStyle = colors.trackColor;
            ctx.fillRect(-graphHelper.scrollWidth, 0, graphHelper.scrollWidth, visibleHeight);

            ctx.fillStyle = colors.scrollColor;

            const scrollOffset = topScrollOffset * visibleHeight / totalHeight;
            const scrollHeight = visibleHeight * visibleHeight / totalHeight;

            ctx.fillRect(-graphHelper.scrollWidth, scrollOffset, graphHelper.scrollWidth, scrollHeight);

        } finally {
            ctx.restore();
        }

    }

    static drawArrow(ctx: CanvasRenderingContext2D, x: number, y: number, rightArrow: boolean) {
        ctx.beginPath();
        if (rightArrow) {
            ctx.moveTo(x, y);
            ctx.lineTo(x + 7, y + 4);
            ctx.lineTo(x, y + 8);
        } else {
            ctx.moveTo(x, y + 1);
            ctx.lineTo(x + 4, y + 8);
            ctx.lineTo(x + 8, y + 1);
        }
        ctx.fill();
    }

    static timeRangeFromSortedRanges(input: Array<[Date, Date]>): [Date, Date] {
        if (input.length === 0) {
            return null;
        }

        const minDate = input[0][0];
        const maxDate = input[input.length - 1][1];
        return [minDate, maxDate];
    }

    /**
     * Divide elements
     * Ex. For Total width = 100, elementWidth = 20, elements = 2
     * We have:
     * | 20px padding | 20px element | 20px padding | 20px element | 20px padding |
     * So elements width stays the same and padding is divided equally,
     * We return start X (which in 20 px in this case)
     * and offset - as width between objects start (40px)
     */
    static computeStartAndOffset(totalWidth: number, elements: number, elementWidth: number): { start: number; offset: number } {
        const offset = (totalWidth - elementWidth * elements) / (elements + 1) + elementWidth;
        const start = offset - elementWidth;

        return {
            offset: offset,
            start: start
        };
    }

    static layoutUsingNearestCenters(items: Array<{ x: number, width: number }>, padding: number) {
        if (items.length === 0) {
            return;
        }

        const desiredX = items.map(item => item.x);

        const mapping = new Map<number, number>();

        _.sortBy(items.map((item, idx) => ({ idx: idx, value: item.x })), x => x.value).forEach((v, k) => {
            mapping.set(k, v.idx);
        });

        const getItem = (idx: number) => {
            return items[mapping.get(idx)];
        }

        const getDesiredX = (idx: number) => {
            return desiredX[mapping.get(idx)];
        }

        getItem(0).x = getDesiredX(0) - getItem(0).width / 2;

        const emptySpaceInfo = [] as Array<{ space: number, idx: number }>;

        let currentPosition = getItem(0).x + getItem(0).width + padding;

        for (let i = 1; i < items.length; i++) {
            let item = getItem(i);
            let requestedX = getDesiredX(i);

            if (requestedX - item.width / 2 >= currentPosition) {
                item.x = requestedX - item.width / 2;
                currentPosition = item.x + item.width + padding;
                const prevItem = getItem(i - 1);
                const emptySpace = item.x - (prevItem.x + prevItem.width);
                emptySpaceInfo.push({
                    space: emptySpace - padding,
                    idx: i
                });
            } else {
                // move items to left
                item.x = currentPosition;

                const xShift = currentPosition - requestedX + item.width / 2;
                let startMoveIdx = 0;
                let avgShift = 0;
                let done = false;
                while (!done) {
                    if (emptySpaceInfo.length > 0) {
                        const space = emptySpaceInfo[emptySpaceInfo.length - 1];
                        startMoveIdx = space.idx;
                        avgShift = xShift * 1.0 / (i - startMoveIdx + 1);
                        if (avgShift < space.space) {
                            // we have more space then we need
                            space.space -= avgShift;
                            done = true;
                        } else {
                            avgShift = space.space;
                            emptySpaceInfo.pop();
                        }
                    } else {
                        // move all elements
                        startMoveIdx = 0;
                        avgShift = xShift * 1.0 / (i + 1);
                        done = true;
                    }

                    for (var j = startMoveIdx; j <= i; j++) {
                        getItem(j).x -= avgShift;
                    }

                    currentPosition = item.x + item.width + padding;
                }
            }
        }
    }

    private static readonly arrowConfig = {
        halfWidth: 6,
        height: 8,
        straightLine: 7  
    }

    static drawBezierDiagonal(ctx: CanvasRenderingContext2D, source: [number, number], target: [number, number], withArrow = false) {
        ctx.beginPath();

        const m = (source[1] + target[1]) / 2;

        if (source[1] < target[1]) {
            ctx.moveTo(source[0], source[1]);
            ctx.lineTo(source[0], source[1] + graphHelper.arrowConfig.straightLine);
            ctx.bezierCurveTo(source[0], m, target[0], m, target[0], target[1] - graphHelper.arrowConfig.straightLine);
            ctx.lineTo(target[0], target[1]);
            ctx.stroke();
        } else {
            ctx.moveTo(source[0], source[1]);
            ctx.lineTo(source[0], source[1] - graphHelper.arrowConfig.straightLine);
            ctx.bezierCurveTo(source[0], m, target[0], m, target[0], target[1] + graphHelper.arrowConfig.straightLine);
            ctx.lineTo(target[0], target[1]);
            ctx.stroke();
        }

        if (withArrow) {
            ctx.beginPath();
            ctx.moveTo(target[0] - graphHelper.arrowConfig.halfWidth, target[1] + graphHelper.arrowConfig.height);
            ctx.lineTo(target[0], target[1]);
            ctx.lineTo(target[0] + graphHelper.arrowConfig.halfWidth, target[1] + graphHelper.arrowConfig.height);
            ctx.stroke();
        }
    }

    static circleLayout(nodes: Array<layoutable>, radius: number) {

        if (nodes.length === 1) {
            nodes[0].x = 0;
            nodes[0].y = 0;
        } else {
            nodes.forEach((node: layoutable, idx: number) => {
                node.x = radius * Math.sin(idx * 2 * Math.PI / nodes.length);
                node.y = radius * Math.cos(Math.PI - idx * 2 * Math.PI / nodes.length);
            });
        }
    }

    static pairIterator<T>(elements: Array<T>, callback: (left: T, right: T) => void) {
        if (elements.length < 2) {
            return;
        }
        for (let i = 0; i < elements.length; i++) {
            for (let j = 0; j < elements.length; j++) {
                if (i !== j) {
                    callback(elements[j], elements[i]);
                }
            }
        }
    }

    static shortenLineFromObject(line: { source: { x: number; y: number }; target: { x: number; y: number }}, radius: number) {
        return graphHelper.shortenLine(line.source.x, line.source.y, line.target.x, line.target.y, radius);
    }

    static shortenLine(x1: number, y1: number, x2: number, y2: number, radius: number) {
        let dx = x2 - x1;
        let dy = y2 - y1;

        const length = Math.sqrt(dx * dx + dy * dy);
        if (length > 0) {
            dx /= length;
            dy /= length;
        }

        if (radius * 2 > length) {
            // return point which is average
            return {
                x1: (x1 + x2) / 2,
                x2: (x1 + x2) / 2,
                y1: (y1 + y2) / 2,
                y2: (y1 + y2) / 2
            };
        }

        dx *= radius;
        dy *= radius;
        

        return {
            x1: x1 + dx,
            x2: x2 - dx,
            y1: y1 + dy,
            y2: y2 - dy
        };
    } 

    static createArrow(container: d3.Selection<void>, type: "start" | "end", fill: string, id: string) {
        if (type === "start") {
            container.append("marker")
                .attr({
                    id: id,
                    markerWidth: 15,
                    markerHeight: 17,
                    refX: 6,
                    refY: 8.5,
                    orient: "auto",
                    markerUnits: "userSpaceOnUse",
                    viewBox: "0 0 15 17"
                })
                .append("path")
                .attr({
                    d: "M13,2 L13,15 L2,8.5 z",
                    fill: fill
            });
        } else {
            container.append("marker")
                .attr({
                    id: id,
                    markerWidth: 15,
                    markerHeight: 17,
                    refX: 9,
                    refY: 8.5,
                    orient: "auto",
                    markerUnits: "userSpaceOnUse",
                    viewBox: "0 0 15 17"
                })
                .append("path")
                .attr({
                    d: "M2,2 L2,15 L13,8.5 z",
                    fill: fill
                });
        }
    }
    
    static movePoints(start: { x: number, y: number }, end: {x: number, y: number }, delta: number): [{ x: number, y: number }, { x: number, y: number }] {
        let x1 = start.x;
        let y1 = start.y;
        let x2 = end.x;
        let y2 = end.y;

        const sign = Math.sign(y1 - y2);
        const coeff = sign ? (-x2 + x1) / (y2 - y1) : (-x2 + x1) / 1e-6;
        const alpha = Math.atan(coeff);
        
        const dx = sign * delta * Math.cos(alpha);
        const dy = sign * delta * Math.sin(alpha);
        
        return [
            {
                x: x1 + dx,
                y: y1 + dy
            }, {
                x: x2 + dx,
                y: y2 + dy
            }
        ];
    }
}

export = graphHelper;
