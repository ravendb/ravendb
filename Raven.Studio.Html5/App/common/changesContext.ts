/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

import changesApi = require("common/changesApi");

class changesContext {
    static currentResourceChangesApi = ko.observable<changesApi>(null);
}

export = changesContext;
