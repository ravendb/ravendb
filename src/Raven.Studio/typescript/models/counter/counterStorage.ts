import EVENTS = require("common/constants/events");
import resource = require("models/resources/resource");

class counterStorage extends resource {
    static type = "counterstorage";
    static readonly qualifier = "cs";

    constructor(name: string, isAdminCurrentTenant: boolean, isDisabled: boolean, bundles: string[] = []) {
        super(name, isAdminCurrentTenant, isDisabled, bundles);
        /* TODO:
        this.isLicensed = ko.computed(() => {
            if (!!license.licenseStatus() && license.licenseStatus().IsCommercial) {
                var counterStorageValue = license.licenseStatus().Attributes.counterStorage;
                return /^true$/i.test(counterStorageValue);
            }
            return true;
        });*/
    }

    get qualifier() {
        return counterStorage.qualifier;
    }

    get urlPrefix() {
        return "cs";
    }

    get fullTypeName() {
        return "Counter Storage";
    }

    get type() {
        return counterStorage.type;
    }

} 

export = counterStorage; 
