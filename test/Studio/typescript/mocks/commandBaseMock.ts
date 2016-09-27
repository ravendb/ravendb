/// <reference path="../../../../src/Raven.Studio/typings/tsd.d.ts" />

import commandBase = require("src/Raven.Studio/typescript/commands/commandBase");
import resource = require("src/Raven.Studio/typescript/models/resources/resource");


class commandBaseMock extends commandBase {
    protected ajax(relativeUrl: string, args: any, method: string, resource?: resource, options?: JQueryAjaxSettings, timeToAlert: number = 9000): JQueryPromise<any> {
        throw new Error("Command execution is not supported during tests at: " + (<any>this).__moduleId__);
    }
}

export = commandBaseMock;
