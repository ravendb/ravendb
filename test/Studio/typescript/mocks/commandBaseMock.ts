/// <reference path="../../../../src/Raven.Studio/typings/tsd.d.ts" />

import commandBase = require("src/Raven.Studio/typescript/commands/commandBase");
import resource = require("src/Raven.Studio/typescript/models/resources/resource");


class commandBaseMock extends commandBase {

    static errorQueue = [] as Array<string>;

    protected ajax(relativeUrl: string, args: any, method: string, resource?: resource, options?: JQueryAjaxSettings, timeToAlert: number = 9000): JQueryPromise<any> {
        const errorMsg = "Command execution is not supported during tests at: " + (<any>this).__moduleId__;
        commandBaseMock.errorQueue.push(errorMsg);
        throw new Error(errorMsg);
    }
}

export = commandBaseMock;
