/// <reference path="../../Scripts/typings/jstree/jstree.d.ts" />

import composition = require("durandal/composition");
import appUrl = require("common/appUrl");
import getFoldersCommand = require("commands/filesystem/getFoldersCommand");

/*
 * A custom Knockout binding handler transforms the target element (a <div>) into a tree, powered by jsTree
 * Usage: data-bind="tree: { value: someObservableTreeObject }"
 */
class treeBindingHandler {

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
    init(element: HTMLElement, valueAccessor, allBindings, viewModel, bindingContext: any) {
        var selectedNode = valueAccessor();

        var tree = $(element).jstree({
            'core': {
                'data': function (node, callback) {
                    var command;
                    if (node && node.id === "#") {
                        command = new getFoldersCommand(appUrl.getFilesystem(), 0, 100);
                    }
                    else {
                        command = new getFoldersCommand(appUrl.getFilesystem(), 0, 100, node);
                    }
                    command.execute().done((nodes: string[]) => {
                        callback.call(this, nodes)
                    });
                }
            }
        });

        tree.on('changed.jstree', function (e, data) {
            selectedNode(data.instance.get_node(data.selected[0]).text);
        });
    }

    // Called by Knockout each time the dependent observable value changes.
    update(element: HTMLElement, valueAccessor, allBindings, viewModel, bindingContext: any) {
        var selectedFolder: string = valueAccessor();
        if (selectedFolder) {
            var selectedNode = $.jstree.reference($(element)).get_selected();
            if (selectedNode) {
                $.jstree.reference($(element)).create_node(selectedNode[0] ? selectedNode[0] : null, { id: selectedFolder });
            }
        }
    }
}

export = treeBindingHandler;