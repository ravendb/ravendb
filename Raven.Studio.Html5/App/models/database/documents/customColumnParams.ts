import execJs = require("common/execJs");

class customColumnParams {

    header = ko.observable<string>();
    binding = ko.observable<string>();
    template = ko.observable<string>();
    width = ko.observable<number>();

    bindingCustomValidity = ko.computed(() => {
        var binding = this.binding();
        return execJs.validateSyntax(binding) || "";
    });

    constructor(dto: customColumnParamsDto) {
        this.header(dto.Header);
        this.binding(dto.Binding);
        this.template(dto.Template || "defaultTemplate");
        this.width(dto.DefaultWidth);
    }

    static empty() {
        return new customColumnParams({ Header: '', Binding: '', DefaultWidth: 200 });
    }

    toDto() {
        return {
            'Header': this.header(),
            'Binding': this.binding(),
            'DefaultWidth': this.width(),
        };
    }

}

export = customColumnParams;