class cell {
    data = ko.observable<any>();

    static defaultTemplate = "defaultTemplate";
    static idTemplate = "idTemplate";
    static checkboxTemplate = "checkboxTemplate";

    constructor(data: any, public templateName: string) {
        this.data(data);
    }

    reset() {
        this.data('');
    }
}

export = cell;