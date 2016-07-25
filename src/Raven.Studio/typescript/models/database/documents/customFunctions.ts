import documentMetadata = require("models/database/documents/documentMetadata");

class customFunctions {
    functions: string;
    __metadata: documentMetadata;

    constructor(dto?: customFunctionsDto) {
        if (!dto) {
            dto = {
                Functions: ""
            };
            this.__metadata = new documentMetadata();
        } else {
            this.__metadata = new documentMetadata((<any>dto)["@metadata"]);
        }

        this.functions = dto.Functions;
    }

    toDto(includeMetadata?: boolean): customFunctionsDto {
        var dto: customFunctionsDto = {
            Functions: this.functions
        };

        if (includeMetadata && this.__metadata) {
            (<any>dto)['@metadata'] = this.__metadata.toDto();
        }
        return dto;
    }

    clone(): customFunctions {
        var copy = new customFunctions(this.toDto());
        copy.functions = this.functions;
        return copy;
    }

    static empty(): customFunctions {
        return new customFunctions({
            Functions: ""
        });
    }
}

export = customFunctions;
