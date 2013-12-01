class column {

    width = ko.observable(0);

    constructor(public name: string, width: number) {
        this.width(width);
    }
}

export = column;