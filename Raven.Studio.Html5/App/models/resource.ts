class resource {
    isSystem = false;
    isSelected = ko.observable<boolean>(false);
    isChecked = ko.observable<boolean>(false);
    itemCount: KnockoutComputed<number>;
    itemCountText: KnockoutComputed<string>;
    isVisible = ko.observable(true);
    disabled = ko.observable<boolean>(false);
    isLicensed: KnockoutComputed<boolean>;

    constructor(public name: string, public type: string) {
    }

    activate() {
        throw new Error("Activate must be overridden.");
    }

    checkboxToggle() {
        this.isChecked.toggle();
    }

    isDatabase() {
        return this.type == "database";
    }

    isFilesystem() {
        return this.type == 'filesystem';
    }
}

export = resource;