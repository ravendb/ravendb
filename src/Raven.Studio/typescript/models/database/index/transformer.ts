/// <reference path="../../../../typings/tsd.d.ts" />

import appUrl = require("common/appUrl");

class transformer {
    public name = ko.observable<string>().extend({ required: true });
    public transformResults = ko.observable<string>().extend({ required: true });
    public lockMode = ko.observable<string>();
    private originalName = ko.observable<string>();
    public wasNameChanged = ko.computed<boolean>(()=> this.name() != this.originalName());
    
    editUrl: KnockoutComputed<string>;
    

    initFromLoad(dto: transformerDto): transformer {
        this.originalName(dto.name.toString());
        this.name(dto.name);
        this.editUrl = appUrl.forCurrentDatabase().editTransformer(this.name());
        this.transformResults(dto.definition.TransformResults);
        this.lockMode(dto.definition.LockMode);
        return this;
    }

    initFromSave(dto: savedTransformerDto): transformer{
        this.originalName(dto.Transformer.Name.toString());
        this.name(dto.Transformer.Name);
        this.editUrl = appUrl.forCurrentDatabase().editTransformer(this.name());
        this.transformResults(dto.Transformer.TransformResults);
        this.lockMode(dto.Transformer.LockMode);
        return this;
    }

    toDto(): transformerDto {
        return {
            'name': this.name(),
            'definition': {
                'Name': this.name(),
                'TransformResults': this.transformResults(),
                'LockMode': this.lockMode()
            }
        };
    }

    toSaveDto(): saveTransformerDto {
        return {
            'Name': this.name(),
            'TransformResults': this.transformResults()
        };
    }

    extractInputs(): Array<transformerParamInfo> {
        var matcher = /(Query|Parameter)\(["'].*?["']\)/g;
        var defaultMatcher = /(Query|Parameter)OrDefault\(["'].*?["'],\s+["'].*?["']\)/g;
        if (this.transformResults()) {
            var parameters: string[] = this.transformResults().match(matcher);
            var parametersWithDefault: string[] = this.transformResults().match(defaultMatcher);
            var results: Array<{name : string, hasDefault: boolean}> = [];

            if (parameters !== null) {
                parameters.forEach((value: string) => results.push( {
                    name: value.substring(value.indexOf('(') + 2, value.length - 2),
                    hasDefault: false
                }));
            }

            if (parametersWithDefault !== null) {
                parametersWithDefault.forEach((value: string) => results.push({
                    name: value.substring(value.indexOf('(') + 2, value.indexOf(',') - 1),
                    hasDefault: true
                }));
            }
            return results;
        }

        return [];
    }
    
    static empty(): transformer{
        return new transformer().initFromLoad({
            'name': "",
            'definition': {
                'Name': "",
                'TransformResults': "",
                'LockMode':"Unlock"
            }
        });
    }

    isLocked(): boolean {
        return this.lockMode() === "LockedIgnore";
    }
}

export = transformer;
