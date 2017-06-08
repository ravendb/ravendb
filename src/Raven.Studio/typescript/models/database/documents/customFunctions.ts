/// <reference path="../../../../typings/tsd.d.ts"/>

class customFunctions {
    functions: string;

    constructor(dto: Raven.Server.Documents.CustomFunctions) {
        this.functions = dto.Functions;
    }

    toDto(): Raven.Server.Documents.CustomFunctions {
        return {
            Functions: this.functions
        };
    }

    get hasEmptyScript() {
        // check if scripts contains any non-whitespace character and inverse condition 
        return !(/\S/.test(this.functions));
    }

    static empty(): customFunctions {
        return new customFunctions({
            Functions: ""
        });
    }
}

export = customFunctions;