/// <reference path="../models/dto.ts" />

import appUrl = require("common/appUrl");


class transformer {
    public name = ko.observable<string>();

    private originalName = ko.observable<string>();

    public wasNameChanged = ko.computed<boolean>(()=> this.name() != this.originalName());
    public transformResults = ko.observable<string>();
    editUrl: KnockoutComputed<string>;
    

    initFromLoad(dto: transformerDto): transformer {
        this.originalName(dto.name.toString());
        this.name(dto.name);
        this.editUrl = appUrl.forCurrentDatabase().editTransformer(encodeURIComponent(this.name()));
        this.transformResults(dto.definition.TransformResults);
        return this;
    }

    initFromSave(dto: savedTransformerDto): transformer{
        this.originalName(dto.Transformer.Name.toString());
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