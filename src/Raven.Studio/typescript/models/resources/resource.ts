/// <reference path="../../../typings/tsd.d.ts"/>

import EVENTS = require("common/constants/events");

abstract class resource {
    isAdminCurrentTenant = ko.observable<boolean>(false);
    activeBundles = ko.observableArray<string>();
    name: string;
    disabled: boolean;

    protected constructor(name: string, isAdminCurrentTenant: boolean, disabled: boolean, activeBundles: string[]) {
        this.name = name;
        this.isAdminCurrentTenant(isAdminCurrentTenant);
        this.activeBundles(activeBundles);
        this.disabled = disabled;
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

    updateUsing(incomingCopy: this) {
        this.isAdminCurrentTenant = incomingCopy.isAdminCurrentTenant;
        this.activeBundles = incomingCopy.activeBundles;
        this.name = incomingCopy.name;
    }

    get qualifiedName() {
        return this.qualifier + "/" + this.name;
    }
}

export = resource;
