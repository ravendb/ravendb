class cell {
    data = ko.observable<any>();

    static defaultTemplate = "defaultTemplate";
    static idTemplate = "idTemplate";
    static checkboxTemplate = "checkboxTemplate";
    static externalIdTemplate = "externalIdTemplate";
    static customTemplate = "customTemplate";

    constructor(data: any, public templateName: string) {
        this.data(data);
    }

    reset() {
        this.data('');
    }
}

export = cell;