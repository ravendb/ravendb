import resource = require("models/resources/resource");
import license = require("models/auth/license");
import counterStorageStatistics = require("models/counter/counterStorageStatistics");

class counterStorage extends resource{
    statistics = ko.observable<counterStorageStatistics>();
    static type = "counterstorage";
    iconName = "fa fa-sort-numeric-desc";

    constructor(name: string, isAdminCurrentTenant: boolean = true, isDisabled: boolean = false) {
        super(name, TenantType.CounterStorage, isAdminCurrentTenant);
        this.fullTypeName = "Counter Storage";
        this.disabled(isDisabled);
        this.itemCountText = ko.computed(() => !!this.statistics() ? this.statistics().counterCountText() : "");

        this.isLicensed = ko.computed(() => {
            if (!!license.licenseStatus() && license.licenseStatus().IsCommercial) {
                var counterStorageValue = license.licenseStatus().Attributes.counterStorage;
                return /^true$/i.test(counterStorageValue);
            }
            return true;
        });
    }

    activate() {
        this.isLoaded(true);
        ko.postbox.publish("ActivateCounterStorage", this);
    }

    saveStatistics(dto: counterStorageStatisticsDto) {
        if (!this.statistics()) {
            this.statistics(new counterStorageStatistics());
        }

        this.statistics().fromDto(dto);
    }
} 

export = counterStorage; 
