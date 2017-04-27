/// <reference path="../../../../typings/tsd.d.ts" />

import appUrl = require("common/appUrl");

class transformer {
    name = ko.observable<string>();
    transformResults = ko.observable<string>();
    lockMode = ko.observable<Raven.Client.Documents.Transformers.TransformerLockMode>();
    temporary = ko.observable<boolean>();
    etag = ko.observable<number>();

    editUrl: KnockoutComputed<string>;
    isLocked = ko.pureComputed(() => this.lockMode() === "LockedIgnore");

    filteredOut = ko.observable<boolean>(false); //UI only property
    
    constructor(dto: Raven.Client.Documents.Transformers.TransformerDefinition) {
        this.updateUsing(dto);

        this.initializeObservables();
    }

    cleanForClone() {
        this.name(null);
        this.lockMode("Unlock");
        this.temporary(false);
    }

    updateUsing(dto: Raven.Client.Documents.Transformers.TransformerDefinition) {
        this.name(dto.Name);
        this.lockMode(dto.LockMode);
        this.transformResults(dto.TransformResults);
        this.etag(dto.Etag);
        this.filteredOut(false);
    }

    private initializeObservables() {
        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editTransformer(this.name());
    }

    toDto(): Raven.Client.Documents.Transformers.TransformerDefinition {
        return {
            LockMode: this.lockMode(),
            Name: this.name(),
            TransformResults: this.transformResults(),
            Etag: this.etag()
        }
    }

    static extractInputs(transformResult: string): Array<transformerParamInfo> {
        if (!transformResult) {
            return [];
        }

        const matcher = /Parameter\(["'].*?["']\)/g;
        const defaultMatcher = /ParameterOrDefault\(["'].*?["'],\s+["'].*?["']\)/g;

        const parameters: string[] = transformResult.match(matcher);
        const parametersWithDefault: string[] = transformResult.match(defaultMatcher);
        const results: Array<transformerParamInfo> = [];

        if (parameters) {
            parameters.forEach((value: string) => results.push({
                name: value.substring(value.indexOf("(") + 2, value.length - 2),
                hasDefault: false
            }));
        }

        if (parametersWithDefault) {
            parametersWithDefault.forEach((value: string) => results.push({
                name: value.substring(value.indexOf("(") + 2, value.indexOf(",") - 1),
                hasDefault: true
            }));
        }
        return results;
    }
    
    static empty(): transformer {
        return new transformer({
            LockMode: "Unlock",
            Name: "",
            TransformResults: "",
            Etag: 0
        } as Raven.Client.Documents.Transformers.TransformerDefinition);
    }
    
}

export = transformer;
