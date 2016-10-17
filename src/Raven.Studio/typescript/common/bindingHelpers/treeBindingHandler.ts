/// <reference path="../../../typings/tsd.d.ts" />

import composition = require("durandal/composition");
import appUrl = require("common/appUrl");
import getFoldersCommand = require("commands/filesystem/getFoldersCommand");
import filesystem = require("models/filesystem/filesystem");

/*
 * A custom Knockout binding handler transforms the target element (a <div>) into a directory tree, powered by jquery-dynatree
 * Usage: data-bind="tree: { value: someObservableTreeObject }"
 */
type bindingOptions = {
    selectedNode: KnockoutObservable<string>;
    addedNode: KnockoutObservable<string>;
    currentLevelNodes: KnockoutObservableArray<string>;
};

class treeBindingHandler {

    static transientNodeStyle = "temp-folder";

    static includeRevisionsFunc: () => boolean;

    static install() {
        if (!ko.bindingHandlers["tree"]) {
            ko.bindingHandlers["tree"] = new treeBindingHandler();

            // This tells Durandal to fire this binding handler only after composition 
            // is complete and attached to the DOM.
            // See http://durandaljs.com/documentation/Interacting-with-the-DOM/
            composition.addBindingHandler("tree");
        }
    }

    // Called by Knockout a single time when the binding handler is setup.
    init(element: HTMLElement, valueAccessor: () => KnockoutObservable<bindingOptions>, allBindings: any, viewModel: any, bindingContext: any) {
        var options: bindingOptions = ko.utils.unwrapObservable(valueAccessor());

        var tree = $(element).dynatree({
            children: [{ title: appUrl.getFileSystemNameFromUrl(), key: "/", isLazy: true, isFolder: true }],
            onLazyRead: function (node) {
                treeBindingHandler.loadNodeChildren("#" + element.id, node, options);
                node.activate();
            },
            onExpand: function (expanded, node) {
                if (expanded) {
                    treeBindingHandler.loadNodeChildren("#" + element.id, node, options);
                    node.activate();
                }
            },
            selectMode: 1,
            onSelect: function (flag, node) {
                treeBindingHandler.onActivateAndSelect(node, valueAccessor());
            },
            onActivate: function (node) {
                treeBindingHandler.onActivateAndSelect(node, valueAccessor());
            },
        });

        var firstNode = (<DynaTreeNode>$(element).dynatree("getRoot", [])).getChildren()[0];
        firstNode.activate();
        firstNode.expand(null);
    }

    static loadNodeChildren(tree: string, node: DynaTreeNode, options: bindingOptions) {
        var dir: string;
        if (node && node.data && node.data.key != "/") {
            dir = node.data.key;
        }
        var command = new getFoldersCommand(new filesystem(appUrl.getFileSystemNameFromUrl()), 0, 1024, dir); //TODO: pass fs using injection or remove fs specific code from this class
        command.execute().done((results: folderNodeDto[]) => {
            node.setLazyNodeStatus(0);

            var versioningEnabled = this.includeRevisionsFunc(); 
            if (!dir && versioningEnabled) {
                results.unshift({ key: "/$$revisions$$", title: "$revisions", isLazy: false, isFolder: true });
            }

            var newSet: { [key: string]: folderNodeDto; } = {};
            var differenceSet: { [key: string]: DynaTreeNode; } = {};


            //this is being done creating a new dictionary to reduce complexity

            for (var i = 0; i < results.length; i++) {
                newSet[results[i].key] = results[i];
            }

            if (node.hasChildren()) {
                //calculate deleted and transient nodes
                for (var j = 0; j < node.getChildren().length; j++) {
                    if (!newSet[node.getChildren()[j].data.key]) {
                        differenceSet[node.getChildren()[j].data.key] = node.getChildren()[j];
                    }
                }

                var nodesToRemove: number[] = [];
                //mark deleted nodes filtering transient
                for (var k = 0; k < node.getChildren().length; k++) {
                    var nodeK = node.getChildren()[k];
                    if (!newSet[nodeK.data.key] && differenceSet[nodeK.data.key] && differenceSet[nodeK.data.key].data.addClass !== treeBindingHandler.transientNodeStyle) {
                        nodesToRemove.push(k);
                    }
                    else {
                        newSet[nodeK.data.key] = null;
                    }
                }

                //remove deleted nodes
                for (var m = 0; m < nodesToRemove.length; m++) {
                    node.getChildren()[nodesToRemove[m]].remove();
                }
            }


            //add new nodes
            for (var key in newSet) {
                if (newSet[key]) {
                    node.addChild(newSet[key]);
                }
            }

            if (options && node.hasChildren()) {
                var keys: string[] = node.getChildren().map(x => x.data.key);
                options.currentLevelNodes.pushAll(keys);
            }
        });
    }

    static reloadNode(tree: string, nodeKey: string) {
        var dynaTree = $(tree).dynatree("getTree");
        var node = dynaTree.getNodeByKey(nodeKey);

        treeBindingHandler.loadNodeChildren(tree, node, null)
    }

    static onActivateAndSelect(node: DynaTreeNode, valueAccessor: any) {
        var options: {
            selectedNode: KnockoutObservable<string>;
            addedNode: KnockoutObservable<string>;
            currentLevelNodes: KnockoutObservableArray<string>;
        } = <any>ko.utils.unwrapObservable(valueAccessor);

        var selectedNode = node.data && node.data.key != "/" ? node.data.key : null;
        options.selectedNode(selectedNode);
        if (node.data) {
            var siblings: DynaTreeNode[] = [];
            if (node.hasChildren()) {
               siblings = node.getChildren();
            }
            var mappedNodes = siblings.map(x => x.data.title);
            options.currentLevelNodes(mappedNodes);
        }
    }

    // Called by Knockout each time the dependent observable value changes.
    update(element: HTMLElement, valueAccessor: () => KnockoutObservable <bindingOptions>, allBindings: any, viewModel: any, bindingContext: any) {
        var options: {
            selectedNode: KnockoutObservable<string>;
            addedNode: KnockoutObservable<folderNodeDto>;
            currentLevelNodes: KnockoutObservableArray<string>;
        } = <any>ko.utils.unwrapObservable(valueAccessor());
        if (options.addedNode()) {
            var newNode = options.addedNode();
            var activeNode = <DynaTreeNode>$(element).dynatree("getActiveNode", []);
            var parentOfNewNode = newNode.key.substring(0, newNode.key.lastIndexOf("/"));
            if (parentOfNewNode === "") {
                parentOfNewNode = "/";
            }
            var parentNode = $(element).dynatree("getTree").getNodeByKey(parentOfNewNode);
            if (parentNode) {
                parentNode.addChild(newNode);
                if (parentNode == activeNode) {
                    options.currentLevelNodes(activeNode.getChildren().map(x => x.data.title));
                }
                options.addedNode(null);
            }
        }
    }

    static updateNodeHierarchyStyle(tree: string, key: string, styleClass?: string) {
        var theTree = $(tree).dynatree("getTree");
        var slashPosition = key.length;
        while (slashPosition > 0) {
            key = key.substring(0, slashPosition);
            var temporaryNode = theTree.getNodeByKey(key);
            if (temporaryNode && temporaryNode.data.addClass != styleClass) {
                temporaryNode.data.addClass = styleClass
                temporaryNode.reloadChildren();
            }
            slashPosition = key.lastIndexOf("/");
        }
    }

    static setNodeLoadStatus(tree: string, key: string, status: number) {
        // Set node load status
        // -1 = error
        // 0 = load OK
        // 1 = loading
        var object = $(tree);
        if (!object || object.length <= 0)
            return;

        var theTree = object.dynatree("getTree");
        if (!theTree) {
            return;
        }

        var slashPosition = key.length;
        while (slashPosition > 0) {
            key = key.substring(0, slashPosition);
            var temporaryNode = theTree.getNodeByKey(key);
            if (temporaryNode) {
                temporaryNode.setLazyNodeStatus(status);
            }

            slashPosition = key.lastIndexOf("/");
        }
    }

    static nodeExists(tree: string, key: string) : boolean {
        var dynaTree = $(tree).dynatree("getTree");
        var node = dynaTree.getNodeByKey(key);

        if (node) {
            return true;
        }

        return false;
    }

    static isNodeExpanded(tree: string, key: string): boolean {
        var dynaTree = $(tree).dynatree("getTree");
        if (!dynaTree)
            return false;
        var node = dynaTree.getNodeByKey(key);

        return node && node.isExpanded();
    }
}

export = treeBindingHandler;
