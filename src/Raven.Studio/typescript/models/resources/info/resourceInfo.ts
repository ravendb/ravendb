/// <reference path="../../../../typings/tsd.d.ts"/>

import resource = require("models/resources/resource");

abstract class resourceInfo {

    name: string;
    disabled = ko.observable<boolean>();

    filteredOut = ko.observable<boolean>(false);

    constructor(dto: Raven.Client.Data.ResourceInfo) {
        this.name = dto.Name;
        this.disabled(dto.Disabled);
    }

    abstract get qualifier(): string;

    abstract get fullTypeName(): string;

    get qualifiedName() {
        return this.qualifier + "/" + this.name;
    }

    abstract asResource(): resource;

}

export = resourceInfo;
