class column {

    width = ko.observable(0);
    binding: string; 
    header: string;

    constructor(binding: string, width: number, header?: string) {
        this.binding = binding;
        this.header = header || binding;
        this.width(width);
    }
}

export = column;