import resource = require("models/resources/resource");
import license = require("models/auth/license");

class counterStorage extends resource{
    //statistics = ko.observable<DatabaseStatistics>();
    resourceSingular = "counter";
    static type = "counterstorage";

    constructor(name: string, isAdminCurrentTenant: boolean = true, isDisabled: boolean = false, bundles: string[] = []) {
        super(name, counterStorage.type, isAdminCurrentTenant);
        this.disabled(isDisabled);
        this.activeBundles(bundles);
        //this.itemCount = ko.computed(() => !!this.statistics() ? this.statistics().counterCount() : 0);
        /*this.itemCountText = ko.computed(() => {
            var itemCount = this.itemCount();
            var text = itemCount.toLocaleString() + " counter";
            if (itemCount !== 1) {
                text += "s";
            }
            return text;
        });*/

        //TODO: change this to match counter storage
        this.isLicensed = ko.computed(() => {
            if (!!license.licenseStatus() && license.licenseStatus().IsCommercial) {
                var counterStorageValue = license.licenseStatus().Attributes.counterStorage;
                return /^true$/i.test(counterStorageValue);
            }
            return true;
        });
    }

    activate() {
        ko.postbox.publish("ActivateCounterStorage", this);
    }
} 

export = counterStorage; 