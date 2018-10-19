/// <reference path="../../../../typings/tsd.d.ts"/>
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");

class customColumn<T> extends textColumn<T> {

    jsCode: string;

    constructor(gridController: virtualGridController<T>, valueAccessorAsJsCode: string, header: string, width: string) {
        super(gridController, customColumn.parseToFunction('return (' + valueAccessorAsJsCode + ')'), header, width);
        this.jsCode = valueAccessorAsJsCode;
    }

    setJsCode(js: string) {
        this.jsCode = js;
        this.valueAccessor = customColumn.parseToFunction('return (' + js + ')');
    }

    static parseToFunction<T>(code: string): (obj: T) => any {
        return new Function(code) as (obj: T) => any;
    }

    /**
     * We assume `this` is our variable in jsCode.
     * Let's extract fields in form: this.{AnyValidField}
     *
     * Ex. this.FirstName + ' from ' + this.Address.City, should return: [FirstName, Address]
     */
    tryGuessRequiredProperties(): string[] {
        const fieldsRegexp = /this\.(\w+)/g;

        const result = [] as string[];

        let match: RegExpExecArray;
        while ((match = fieldsRegexp.exec(this.jsCode)) !== null) {
            result.push(match[1]);
        }

        return _.uniq(result);
    }

    toDto(): virtualColumnDto {
        return {
            type: "custom",
            width: this.width,
            header: this.header,
            serializedValue: this.jsCode
        }
    }

}

export = customColumn;
