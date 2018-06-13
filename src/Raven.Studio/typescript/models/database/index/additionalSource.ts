/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");

class additionalSource {
    name = ko.observable<string>();
    code = ko.observable<string>();

    dirtyFlag: () => DirtyFlag;
    
    static create(name: string, code: string) {
        const item = new additionalSource();
        item.name(name);
        item.code(code);
        return item;
    }

    constructor() {
        this.dirtyFlag = new ko.DirtyFlag([
            this.name
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
}

export = additionalSource;
