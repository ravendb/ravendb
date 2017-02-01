/// <reference path="../../../typings/tsd.d.ts"/>

import EVENTS = require("common/constants/events");

abstract class resource {
    name: string;    

    activeBundles = ko.observableArray<string>();   
    disabled = ko.observable<boolean>(false);
    errored = ko.observable<boolean>(false);
    isAdminCurrentTenant = ko.observable<boolean>(false);

    protected constructor(rsInfo: Raven.Client.Data.ResourceInfo) {
    }

    activate() {
        ko.postbox.publish(EVENTS.Resource.Activate,
            {
                resource: this
            });
    }

    abstract get fullTypeName(): string;

    abstract get qualifier(): string;

    abstract get urlPrefix(): string;

    abstract get type(): string;

    updateUsing(incomingCopy: Raven.Client.Data.ResourceInfo) {
        this.isAdminCurrentTenant(incomingCopy.IsAdmin);
        this.activeBundles(incomingCopy.Bundles);
        this.name = incomingCopy.Name;
        this.disabled(incomingCopy.Disabled);
        this.errored(!!incomingCopy.LoadError);
    }

    get qualifiedName() {
        return this.qualifier + "/" + this.name;
    }
}

export = resource;
