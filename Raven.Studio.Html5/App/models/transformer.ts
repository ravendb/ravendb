/// <reference path="../models/dto.ts" />

import appUrl = require("common/appUrl");


class transformer {
    public name = ko.observable<string>();
    public transformResults = ko.observable<string>();
    editUrl: KnockoutComputed<string>;
    

    initFromLoad(dto: transformerDto): transformer {
        this.name(dto.name);
        this.editUrl = appUrl.forCurrentDatabase().editTransformer(encodeURIComponent(this.name()));
        this.transformResults(dto.definition.TransformResults);
        return this;
    }

    initFromSave(dto: savedTransformerDto) :transformer{
        this.name(dto.Transformer.Name);
        this.editUrl = appUrl.forCurrentDatabase().editTransformer(encodeURIComponent(this.name()));
        this.transformResults(dto.Transformer.TransformResults);
        return this;
    }

    toDto(): transformerDto {
        return {
            'name': this.name(),
            'definition': {
                'Name': this.name(),
                'TransformResults': this.transformResults()
            }
        };
    }

    toSaveDto(): saveTransformerDto {
        return {
            'Name': this.name(),
            'TransformResults': this.transformResults()
        };
    }

    static empty(): transformer{
        return new transformer().initFromLoad({
            'name': "",
            'definition': {
                'Name': "",
                'TransformResults': ""
            }
        });
    }

    

}

export = transformer;