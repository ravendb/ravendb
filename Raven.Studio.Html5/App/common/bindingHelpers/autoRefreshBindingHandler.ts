import composition = require("durandal/composition");
import viewModelBase = require("viewmodels/viewModelBase");
import d3 = require("d3/d3");

interface autoRefreshConfigDto {
    duration: number;
    autoStart: boolean;
    onRefresh: () => JQueryPromise<any>;
}

interface autoRefreshContext extends autoRefreshConfigDto {
    svg: D3.Selection;
    arc: D3.Svg.Arc;
    path: D3.Selection;
    refreshing: KnockoutObservable<boolean>;
    autorefreshEnabled: KnockoutObservable<boolean>;
    disposed: boolean;
}

class autoRefreshBindingHandler {

    static install() {
        if (!ko.bindingHandlers["autoRefresh"]) {
            ko.bindingHandlers["autoRefresh"] = new autoRefreshBindingHandler();

            // This tells Durandal to fire this binding handler only after composition 
            // is complete and attached to the DOM.
            // See http://durandaljs.com/documentation/Interacting-with-the-DOM/
            composition.addBindingHandler('help');
        }
    }

    init(element: HTMLElement, valueAccessor: () => any, allBindings: any, viewModel: viewModelBase, bindingContext: KnockoutBindingContext) {
        var config: autoRefreshConfigDto = ko.unwrap(valueAccessor());

        if (!config.onRefresh) {
            throw new Error("onRefresh attribute is required.");
        }
        if (!config.duration) {
            throw new Error("duration attribute is required.");
        }

        var context = autoRefreshBindingHandler.initRefresh(element, config);

        $(element).click(() => autoRefreshBindingHandler.toggleAutorefresh(context));

        if (config.autoStart) {
            autoRefreshBindingHandler.animatePath(context);
        } else {
            context.refreshing(true);
            context.autorefreshEnabled(false);
        }

        var childBindingContext = bindingContext.createChildContext(context, null, context => ko.utils.extend(context, autoRefreshBindingHandler));
        ko.applyBindingsToDescendants(childBindingContext, element);

        ko.utils.domNodeDisposal.addDisposeCallback(element, () => {
            // break animation (if any)
            context.path
                .transition()
                .duration(0);

            context.disposed = true;
        });

        return { controlsDescendantBindings: true };
    }

    private static initRefresh(element: HTMLElement, config: autoRefreshConfigDto): autoRefreshContext {
        var container = d3.select(element);

        container.classed('auto-refresh', true);

        if (!container.attr('title')) {
            container.attr('title', 'Auto refresh');
        }

        container.append("i")
            .attr({
                'class': 'fa fa-fw',
                'data-bind': "css: { 'fa-pause': autorefreshEnabled(), 'fa-refresh': !autorefreshEnabled() }"
            });

        var divContainer = container.append("div")
            .attr({
                'class': 'circleRefresh',
                'data-bind': "css: { 'timer': autorefreshEnabled(), 'dashed': refreshing() }"
            });

        divContainer.append('svg')
            .attr({
                'class': 'inner-svg',
                'width': '80px',
                'height': '80px',
                'viewBox': '0 0 80 80'
            });

        var svg = container.select("svg");
        var group = svg
            .append('g')
            .attr('transform', 'translate(40,40)');

        group.append('circle')
            .attr({
                'class': 'circleRefresh-circlebg',
                cx: 0,
                cy: 0,
                r: 30
            });

        var path = group
            .append('path')
            .attr({
                'class': 'circleRefresh-circle'
            });

        var arc = d3.svg.arc()
            .innerRadius(30)
            .outerRadius(30)
            .startAngle(0)
            .endAngle(d => d * 2 * Math.PI);

        return {
            svg: svg,
            path: path,
            arc: arc,
            autorefreshEnabled: ko.observable<boolean>(true),
            refreshing: ko.observable<boolean>(false),
            duration: config.duration,
            onRefresh: config.onRefresh,
            autoStart: config.autoStart,
            disposed: false
    };
    }

    private static animatePath(context: autoRefreshContext) {
        context.refreshing(false);
        context.path.transition()
            .duration(context.duration)
            .ease('linear')
            .attrTween("d", () => (t) => context.arc(t))
            .each('end', () => {
                context.refreshing(true);

                context.onRefresh()
                    .always(() => {
                        if (!context.disposed) {
                            this.animatePath(context);    
                        }
                    });
            });
    }

    private static toggleAutorefresh(context: autoRefreshContext) {
        if (context.autorefreshEnabled()) {
            // stop current animation if any
            if (!context.refreshing()) {
                context.path
                    .transition()
                    .duration(0);
            }
        } else {
            // start with refresh and then animate refresh
            context.path
                .attr('d', context.arc(1));
            context.refreshing(true);
            context.onRefresh()
                .always(() => autoRefreshBindingHandler.animatePath(context));
        }
        context.autorefreshEnabled(!context.autorefreshEnabled());
    }

    update(element: HTMLInputElement, valueAccessor: () => any, allBindingsAccessor, viewModel: viewModelBase, bindingContext) {
    }
}

export = autoRefreshBindingHandler;
