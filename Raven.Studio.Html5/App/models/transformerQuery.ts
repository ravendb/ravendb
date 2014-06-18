import transformer = require("models/transformer");
import transformerParam = require("models/transformerParam");

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
            // FIXME: http://issues.hibernatingrhinos.com/issue/RavenDB-2397
            // Change "qp-" to "tp-" here when this is issue is resolved
            var paramsUrl = this.queryParams 
                .map((param: transformerParam) => "qp-" + param.name + "=" + param.value) // transform queryParam array into "qp-NAME=VALUE" strings (also array)
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