/// <reference path="../../../../typings/tsd.d.ts"/>

class clusterNode {
    tag = ko.observable<string>();
    serverUrl = ko.observable<string>();
    type = ko.observable<clusterNodeType>();

    cssIcon = ko.pureComputed(() => {
        const type = this.type();
        switch (type) {
            case "Member":
                return "icon-member";
            case "Promotable":
                return "icon-promotable";
            case "Watcher":
                return "icon-watcher";
        }
    });

    updateWith(incoming: clusterNode) {
        this.tag(incoming.tag());
        this.type(incoming.type());
    }

    static for(tag: string, serverUrl: string, type: clusterNodeType) {
        const node = new clusterNode();
        node.tag(tag);
        node.serverUrl(serverUrl);
        node.type(type);
        return node;
    }
}

export = clusterNode;
