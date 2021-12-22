/// <reference path="../../../typings/tsd.d.ts"/>

import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import Favico = require("Favico");

class favNodeBadge {

    private favicons: Array<any>;

    initialize() {
        
        this.favicons = [];
        
        $("link[rel=icon]").each((idx, el) => {
            this.favicons.push(new Favico({
                position: 'downright',
                bgColor: '#e33572', 
                textColor: '#ffffff',
                element: el
            }));
        });
        
        // we set badge as side of effect of ko.computed
        ko.computed(() => {
            const tag = clusterTopologyManager.default.localNodeTag();
            const count = clusterTopologyManager.default.nodesCount();

            this.favicons.forEach(fav => {
                fav.reset();
                if (tag && count > 1) {
                    fav.badge(tag as any);    
                }
            });
        });
    }
}

export = favNodeBadge;
