class resource {
    isAdminCurrentTenant = ko.observable<boolean>(false);
    isSystem = false;
    isSelected = ko.observable<boolean>(false);
    isChecked = ko.observable<boolean>(false);
    itemCountText: KnockoutComputed<string>;
    isVisible = ko.observable(true);
    isLoading = ko.observable(false);
    isLoaded = ko.observable<boolean>(false);
    disabled = ko.observable<boolean>(false);
    isLicensed: KnockoutComputed<boolean>;
    activeBundles = ko.observableArray<string>();
    isImporting = ko.observable<boolean>(false);
    importStatus = ko.observable<string>("");
    isExporting = ko.observable<boolean>(false);
    exportStatus = ko.observable<string>("");
    statistics: KnockoutObservable<any>;
    fullTypeName: string;

    constructor(public name: string, public type: TenantType, isAdminCurrentTenant: boolean) {
        this.isAdminCurrentTenant(isAdminCurrentTenant);
    }

    activate() {
        throw new Error("Activate must be overridden.");
    }

    checkboxToggle() {
        this.isChecked.toggle();
    }

    isDatabase() {
        return this.type === TenantType.Database;
    }

    isFileSystem() {
        return this.type === TenantType.FileSystem;
    }

    isCounterStorage() {
        return this.type === TenantType.CounterStorage;
    }

    isTimeSeries() {
        return this.type === TenantType.TimeSeries;
    }
}

export = resource;
