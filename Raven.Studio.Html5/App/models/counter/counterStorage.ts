import resource = require("models/resources/resource");
import license = require("models/auth/license");

class counterStorage extends resource{
    static type = 'counterstorage';

    constructor(name: string, isAdminCurrentTenant: boolean = true, isDisabled: boolean = false) {
        super(name, counterStorage.type, isAdminCurrentTenant);
        this.disabled(isDisabled);
        this.name = name;

        //TODO: change this to match counter storage
        this.isLicensed = ko.computed(() => {
            if (!!license.licenseStatus() && license.licenseStatus().IsCommercial) {
                var ravenFsValue = license.licenseStatus().Attributes.ravenfs;
                return /^true$/i.test(ravenFsValue);
            }
            return true;
        });
    }

    activate() {
        ko.postbox.publish("ActivateCounterStorage", this);
    }
} 

export = counterStorage; 