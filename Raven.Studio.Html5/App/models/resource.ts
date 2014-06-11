class resource {
    isSystem = false;
    isSelected = ko.observable<boolean>(false);
    itemCount: KnockoutComputed<number>;
    isVisible = ko.observable(true);
    disabled = ko.observable<boolean>(false);

    constructor(public name: string, public type: string) {
    }
}

export = resource;