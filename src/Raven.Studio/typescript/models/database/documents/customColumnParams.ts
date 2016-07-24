import execJs = require("common/execJs");

class customColumnParams {

    header = ko.observable<string>();
    binding = ko.observable<string>();
    template = ko.observable<string>();
    width = ko.observable<number>();
    isNew = ko.observable<boolean>();
    hasHeaderFocus = ko.observable<boolean>();

    bindingCustomValidity = ko.computed(() => {
        var binding = this.binding();
        return execJs.validateSyntax(binding) || "";
    });

    constructor(dto: customColumnParamsDto, isNew: boolean = false) {
        this.header(dto.Header);
        this.binding(dto.Binding);
        this.template(dto.Template || "defaultTemplate");
        this.width(dto.DefaultWidth);
        this.isNew(isNew);

        this.binding.subscribe((newValue: string) => {
            if (this.isNew() === false) {
                //the header was already modified before
                return;
            }

            this.header(newValue);
        });

        this.header.subscribe(() => {
            if (this.hasHeaderFocus()) {
                this.isNew(false);
            }
        });
    }

    static empty() {
        return new customColumnParams({ Header: '', Binding: '', DefaultWidth: 200 }, true);
    }

    toDto() {
        return {
            'Header': this.header(),
            'Binding': this.binding(),
            'DefaultWidth': this.width()
        };
    }
}

export = customColumnParams;
