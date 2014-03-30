class column {

    width = ko.observable(0);
    name: string; // prop name
    title: string; // title (human readable text)

    constructor(name: string, width: number, title?: string) {
        this.name = name;
        this.title = title || name;
        this.width(width);
    }
}

export = column;