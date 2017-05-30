/// <reference path="../../../../typings/tsd.d.ts"/>

class databaseGroupNode {
    tag = ko.observable<string>();
    serverUrl = ko.observable<string>();
    type = ko.observable<clusterNodeType>();

    cssIcon = ko.pureComputed(() => {
        const type = this.type();
        switch (type) {
            case "Member":
                return "icon-dbgroup-member";
            case "Promotable":
                return "icon-dbgroup-promotable";
            case "Watcher":
                return "icon-dbgroup-watcher";
        }
    });

    updateWith(incoming: databaseGroupNode) {
        this.tag(incoming.tag());
        this.type(incoming.type());
    }

    static for(tag: string, serverUrl: string, type: databaseGroupNodeType) {
        const node = new databaseGroupNode();
        node.tag(tag);
        node.serverUrl(serverUrl);
        node.type(type);
        return node;
    }
}

export = databaseGroupNode;
