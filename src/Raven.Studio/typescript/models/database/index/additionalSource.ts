/// <reference path="../../../../typings/tsd.d.ts"/>

class additionalSource {
    name = ko.observable<string>();
    code = ko.observable<string>();
    
    static create(name: string, code: string) {
        const item = new additionalSource();
        item.name(name);
        item.code(code);
        return item;
    }
}

export = additionalSource;
