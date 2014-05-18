class resource {
    isSystem = false;
    isSelected = ko.observable(false);
    itemCount : KnockoutComputed < number>;
    isVisible = ko.observable(true);

    constructor(public name: string, public type: string) {
    }
}

export = resource; 