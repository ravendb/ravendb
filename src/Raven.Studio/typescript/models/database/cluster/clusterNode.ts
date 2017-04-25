
class clusterNode {
    tag = ko.observable<string>();
    serverUrl = ko.observable<string>();
    type = ko.observable<clusterNodeType>();

    static for(tag: string, serverUrl: string, type: clusterNodeType) {
        const node = new clusterNode();
        node.tag(tag);
        node.serverUrl(serverUrl);
        node.type(type);
        return node;
    }
}

export = clusterNode;
