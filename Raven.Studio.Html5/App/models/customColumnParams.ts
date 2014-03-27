class customColumnParams {

    header = ko.observable<string>();
    binding = ko.observable<string>();
    template = ko.observable<string>();
    width = ko.observable<number>();

    constructor(dto: customColumnParamsDto) {
        this.header(dto.Header);
        this.binding(dto.Binding);
        this.template(dto.Template || "defaultTemplate");
        this.width(dto.DefaultWidth);
    }

}

export = customColumnParams;