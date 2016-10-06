/// <reference path="../../../../typings/tsd.d.ts" />

import appUrl = require("common/appUrl");

class transformer {
    name = ko.observable<string>();
    transformResults = ko.observable<string>();
    lockMode = ko.observable<Raven.Abstractions.Indexing.TransformerLockMode>();
    private originalName = ko.observable<string>();
    temporary = ko.observable<boolean>();
    nameChanged = ko.pureComputed<boolean>(() => this.name() !== this.originalName());
    transformerId = ko.observable<number>();

    editUrl: KnockoutComputed<string>;

    isLocked = ko.pureComputed(() => this.lockMode() === ("LockedIgnore" as Raven.Abstractions.Indexing.TransformerLockMode));

    filteredOut = ko.observable<boolean>(false); //UI only property
    
    constructor(dto: Raven.Abstractions.Indexing.TransformerDefinition) {
        this.originalName(dto.Name);
        this.name(dto.Name);
        this.lockMode(dto.LockMode);
        this.temporary(dto.Temporary);
        this.transformerId(dto.TransfomerId);
        this.transformResults(dto.TransformResults);

        this.initializeObservables();
    }

    private initializeObservables() {
        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editTransformer(this.name());
    }

    toDto(): Raven.Abstractions.Indexing.TransformerDefinition {
        return {
            LockMode: this.lockMode(),
            Name: this.name(),
            Temporary: this.temporary(),
            TransfomerId: this.transformerId(),
            TransformResults: this.transformResults()
        }
    }

    extractInputs(): Array<transformerParamInfo> {
        if (this.transformResults()) {
            const matcher = /(Query|Parameter)\(["'].*?["']\)/g;
            const defaultMatcher = /(Query|Parameter)OrDefault\(["'].*?["'],\s+["'].*?["']\)/g;

            const parameters: string[] = this.transformResults().match(matcher);
            const parametersWithDefault: string[] = this.transformResults().match(defaultMatcher);
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

        return [];
    }
    
    static empty(): transformer {
        return new transformer({
            LockMode: "Unlock",
            Name: "",
            TransformResults: ""
        } as Raven.Abstractions.Indexing.TransformerDefinition);
    }
    
}

export = transformer;
