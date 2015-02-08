class cell {
    data = ko.observable<any>();
    resetFlag = false;

    static defaultTemplate = "defaultTemplate";
    static idTemplate = "idTemplate";
    static checkboxTemplate = "checkboxTemplate";
    static externalIdTemplate = "externalIdTemplate";
    static customTemplate = "customTemplate";

    constructor(data: any, public templateName: string) {
        this.update(data);
    }

    update(data: any) {
        if (this.isNumber(data)) {
            data = data.toLocaleString();
        }
        this.data(data);
    }

    private isNumber(o) {
        return typeof o === 'number' && isFinite(o);
    }

    reset() {
        this.data('');       
        this.resetFlag = true; 
    }
}

export = cell;