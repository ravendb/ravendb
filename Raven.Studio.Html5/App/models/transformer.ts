/// <reference path="../models/dto.ts" />

class transformer {
    public name = ko.observable<string>();
    public transformResults = ko.observable<string>();
    
    initFromLoad(dto: transformerDto): transformer {
        this.name(dto.name);
        this.transformResults(dto.definition.TransformResults);
        return this;
    }

    initFromSave(dto: savedTransformerDto) :transformer{
        this.name(dto.Transformer.Name);
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