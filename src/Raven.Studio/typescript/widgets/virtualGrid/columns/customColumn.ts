/// <reference path="../../../../typings/tsd.d.ts"/>
import textColumn = require("widgets/virtualGrid/columns/textColumn");

class customColumn<T> extends textColumn<T> {

    jsCode: string;

    constructor(valueAccessorAsJsCode: string, header: string, width: string) {
        super(customColumn.parseToFunction(valueAccessorAsJsCode), header, width);
        this.jsCode = valueAccessorAsJsCode;
    }

    static parseToFunction<T>(code: string): (obj: T) => any {
        return new Function('x', code) as (obj: T) => any;
    }

    /**
     * We assume `x` is our variable in jsCode.
     * Let's extract fields in form: x.{AnyValidField}
     *
     * Ex. x.FirstName + ' from ' + x.Address.City, should return: [FirstName, Address]
     */
    tryGuessRequiredProperties(): string[] {
        const fieldsRegexp = /x\.(\w+)/g;

        const result = [] as string[];

        let match: RegExpExecArray;
        while ((match = fieldsRegexp.exec(this.jsCode)) !== null) {
            result.push(match[1]);
        }

        return _.uniq(result);
    }

}

export = customColumn;