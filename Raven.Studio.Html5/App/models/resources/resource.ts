class resource {
    isAdminCurrentTenant = ko.observable<boolean>(false);
    isSystem = false;
    isSelected = ko.observable<boolean>(false);
    isChecked = ko.observable<boolean>(false);
    itemCountText: KnockoutComputed<string>;
    isVisible = ko.observable(true);
    isLoading = ko.observable(false);
    disabled = ko.observable<boolean>(false);
    isLicensed: KnockoutComputed<boolean>;
    activeBundles = ko.observableArray<string>();
    isImporting = ko.observable<boolean>(false);
    importStatus = ko.observable<string>("");
    statistics: KnockoutObservable<any>;

    constructor(public name: string, public type: string, isAdminCurrentTenant: boolean) {
        this.isAdminCurrentTenant(isAdminCurrentTenant);
    }

    activate() {
        throw new Error("Activate must be overridden.");
    }

    checkboxToggle() {
        this.isChecked.toggle();
    }

    isDatabase() {
        return this.type === "database";
    }

    isFileSystem() {
        return this.type === "filesystem";
    }

    isCounterStorage() {
        return this.type === "counterstorage";
    }
}

export = resource;