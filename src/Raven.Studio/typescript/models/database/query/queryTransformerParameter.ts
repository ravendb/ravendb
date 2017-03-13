/// <reference path="../../../../typings/tsd.d.ts"/>
class queryTransformerParameter {
    name: string;
    hasDefault: boolean;
    value = ko.observable<string>();

    validationGroup: KnockoutObservable<any>;

    constructor(param: transformerParamInfo) {
        this.name = param.name;
        this.hasDefault = param.hasDefault;

        this.initValidation();
    }

    private initValidation() {
        if (!this.hasDefault) {
            this.value.extend({
                required: true
            });
        }

        this.validationGroup = ko.validatedObservable({
            value: this.value
        });
    }

    toDto(): transformerParamDto {
        return {
            name: this.name,
            value: this.value()
        }
    }
}

export = queryTransformerParameter; 
