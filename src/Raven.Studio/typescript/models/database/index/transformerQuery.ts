/// <reference path="../../../../typings/tsd.d.ts"/>

import transformerParam = require("models/database/index/transformerParam");

class transformerQuery {
    transformerName: string;
    queryParams: Array<transformerParam>;

    constructor(dto: transformerQueryDto) {
        this.transformerName = dto.transformerName;
        this.queryParams = dto.queryParams;
    }

    addParamByNameAndValue(name: string, value: string) {
        this.queryParams.push(new transformerParam(name, value));
    }

    addParam(param: transformerParam) {
        this.queryParams.push(param);
    }

    toUrl(): string {
         if (this.transformerName) {
            var paramsUrl = this.queryParams 
                .map((param: transformerParam) => "tp-" + param.name + "=" + param.value) // transform transformerParam array into "tp-NAME=VALUE" strings (also array)
                .join("&");

            return "&resultsTransformer=" + this.transformerName + (paramsUrl.length > 0 ? "&" + paramsUrl : "");
        } else {
            return "";
        }
    }

    toDto(): transformerQueryDto {
        return {
            transformerName: this.transformerName,
            queryParams: this.queryParams
        };
    }
}

export = transformerQuery;
