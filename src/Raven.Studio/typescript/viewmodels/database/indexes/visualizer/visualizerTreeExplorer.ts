import graphHelper = require("common/helpers/graph/graphHelper");
import viewHelpers = require("common/helpers/view/viewHelpers");


import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class visualizerTreeExplorer extends dialogViewModelBase {

    private dto: Raven.Server.Documents.Indexes.Debugging.ReduceTreePage;

    constructor(dto: Raven.Server.Documents.Indexes.Debugging.ReduceTreePage) {
        super();

        this.dto = dto;

        //TODO: do you really want raw dto as class property ?
    }
}

export = visualizerTreeExplorer;
