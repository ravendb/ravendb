/// <reference path="../../../typings/tsd.d.ts"/>

import clusterTopologyManager = require("common/shell/clusterTopologyManager");

class favNodeBadge {

    private favicon: any;

    initialize() {
        this.favicon = new Favico({
            position: 'downleft',
            bgColor: '#e33572', textColor: '#ffffff'
        });

        // we set badge as side of effect of ko.computed
        ko.computed(() => {
            const tag = clusterTopologyManager.default.localNodeTag();
            const count = clusterTopologyManager.default.nodesCount();

            this.favicon.reset();

            if (tag && count > 1) {
                this.favicon.badge(tag as any);
            }
        });
    }
}

export = favNodeBadge;