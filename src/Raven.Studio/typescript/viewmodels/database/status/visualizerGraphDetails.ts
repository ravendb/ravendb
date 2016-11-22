import app = require("durandal/app");
import graphHelper = require("common/helpers/graph/graphHelper");

import d3 = require('d3');
import rbush = require("rbush");

class visualizerGraphDetails {

    static margins = {
       //TODO: 
    }

    private totalWidth = 1500; //TODO: use dynamic value
    private totalHeight = 700; //TODO: use dynamic value

    private documents = [] as Array<any>; //TODO: don't use any!
    private trees: Array<any> = [];//TODO: don't use any!

    private canvas: d3.Selection<void>;
    private svg: d3.Selection<void>; //TODO: do we really need svg in here?
    private zoom: d3.behavior.Zoom<void>;

    private dataWidth = 0; // total width of all virtual elements
    private dataHeight = 0; // total heigth of all virtual elements

    private xScale: d3.scale.Linear<number, number>;
    private yScale: d3.scale.Linear<number, number>;

    private viewActive = ko.observable<boolean>(false);
    private gotoMasterViewCallback: () => void;

    init(goToMasterViewCallback: () => void) {
        this.gotoMasterViewCallback = goToMasterViewCallback;

        const container = d3.select("#visualizerContainer");

        this.canvas = container
            .append("canvas")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight);

        this.svg = container
            .append("svg")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight);

        this.toggleUiElements(false);

        this.xScale = d3.scale.linear()
            .domain([0, this.totalWidth])
            .range([0, this.totalWidth]);

        this.yScale = d3.scale.linear()
            .domain([0, this.totalHeight])
            .range([0, this.totalHeight]);

        this.zoom = d3.behavior.zoom<void>()
            .x(this.xScale)
            .y(this.yScale)
            .on("zoom", () => this.onZoom());

        this.svg
            .append("svg:rect")
            .attr("class", "pane")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight)
            .call(this.zoom)
            .call(d => this.setupEvents(d));
    }

    private onZoom() {
        this.draw();
    }

    private setupEvents(selection: d3.Selection<void>) {
        selection.on("dblclick.zoom", null);
        //TODO: allow to click selection.on("click", () => this.onClick());
    }

    private draw() {
        //TODO: !
    }

    goToMasterView() {
        this.viewActive(false);
        this.toggleUiElements(false);
        //TODO: exit animation before calling callback
        this.gotoMasterViewCallback();
    }

    openFor(treeName: string) {
        this.viewActive(true); //TODO: consider setting this after initial animation if any
        this.toggleUiElements(true);
    }

    private toggleUiElements(show: boolean) {
        this.svg.style("display", show ? "block" : "none");
        this.canvas.style("display", show ? "block" : "none");
    }
}

export = visualizerGraphDetails;
