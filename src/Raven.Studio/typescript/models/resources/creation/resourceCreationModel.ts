/// <reference path="../../../../typings/tsd.d.ts"/>

import resource = require("models/resources/resource");
import license = require("models/auth/license");
import configuration = require("configuration");

abstract class resourceCreationModel {
    name = ko.observable<string>("");

    abstract get resourceType(): string;

    activeBundles = ko.observableArray<string>([]);

    dataPath = ko.observable<string>();
    journalsPath = ko.observable<string>();
    tempPath = ko.observable<string>();

    globalValidationGroup = ko.validatedObservable({
        name: this.name
    });

    setupValidation(resourceDoesntExist: (name: string) => boolean) {
        const rg1 = /^[^\\/:\*\?"<>\|]*$/; // forbidden characters \ / : * ? " < > |
        const rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names

        this.setupPathValidation(this.dataPath, "Data");
        this.setupPathValidation(this.tempPath, "Temp");
        this.setupPathValidation(this.journalsPath, "Journals");

        this.name.extend({
            required: true,
            maxLength: 230,
            validation: [
                {
                    validator: resourceDoesntExist,
                    message: _.upperFirst(this.resourceType) + " already exists"
                }, {
                    validator: (val: string) => rg1.test(val),
                    message: `The {0} name can't contain any of the following characters: \\ / : * ? " < > |`,
                    params: this.resourceType
                }, {
                    validator: (val: string) => !val.startsWith("."),
                    message: `The ${this.resourceType} name can't start with a dot!`
                }, {
                    validator: (val: string) => !val.endsWith("."),
                    message: `The ${this.resourceType} name can't end with a dot!`
                }, {
                    validator: (val: string) => !rg3.test(val),
                    message: `The name {0} is forbidden for use!`,
                    params: this.name
                }]
        });
    }

    protected setupPathValidation(observable: KnockoutObservable<string>, name: string) {
        const maxLength = 248;

        const rg1 = /^[^*?"<>\|]*$/; // forbidden characters * ? " < > |
        const rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names

        observable.extend({
            maxLength: {
                params: maxLength,
                message: `Path name for '${name}' can't exceed ${maxLength} characters!`
            },
            validation: [{
                validator: (val: string) => rg1.test(val),
                message: `{0} path can't contain any of the following characters: * ? " < > |`,
                params: name
            },
            {
               validator: (val: string) => !rg3.test(val),
               message: `The name {0} is forbidden for use!`,
               params: this.name
            }]
        });
    }
}

export = resourceCreationModel;
