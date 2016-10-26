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

    encryption = {
        key: ko.observable<string>(), //TODO: validate base64
        algorithm: ko.observable<string>("Rijndael"),
        keyBitsPreference: ko.observable<number>(256),
        encryptIndexes: ko.observable<boolean>(true)
    }

    encryptionAlgorithmsOptions = ["DES", "RC2", "Rijndael", "Triple DES"];
    encryptionKeyBitsOptions = [128, 192, 256];

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
                    message: this.resourceType.capitalizeFirstLetter() + " already exists"
                }, {
                    validator: (val: string) => rg1.test(val),
                    message: `The {0} name can't contain any of the following characters: \ / : * ? " < > |`,
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
        const maxLegnth = 248;
        const rg1 = /^[^*\\?"<>\|]*$/; // forbidden characters \ * : ? " < > |
        const rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names

        observable.extend({
            maxLength: {
                params: maxLegnth,
                message: `The path name for the '${name}' can't exceed ${maxLegnth} characters!`
            },
            validation: [{
                validator: (val: string) => rg1.test(val),
                message: `The {0} can't contain any of the following characters: \\ : * ? " < > |`,
                params: name
            }, {
                validator: (val: string) => !rg3.test(val),
                message: `The name {0} is forbidden for use!`,
                params: this.name
            }]
        });
    }

    protected fillEncryptionSettingsIfNeeded(securedSettings: dictionary<string | boolean>) {
        if (this.activeBundles().contains("Encryption")) {
            securedSettings[configuration.encryption.encryptionKey] = this.encryption.key();
            securedSettings[configuration.encryption.algorithmType] = this.getEncryptionAlgorithmFullName(this.encryption.algorithm());
            securedSettings[configuration.encryption.encryptionKeyBitsPreference] = this.encryption.keyBitsPreference().toString();
            securedSettings[configuration.encryption.encryptIndexes] = this.encryption.encryptIndexes();
        }
    }

    protected getEncryptionAlgorithmFullName(encrytion: string) {
        switch (encrytion) {
            case "DES":
                return "System.Security.Cryptography.DESCryptoServiceProvider, mscorlib";
            case "RC2":
                return "System.Security.Cryptography.RC2CryptoServiceProvider, mscorlib";
            case "Rijndael":
                return "System.Security.Cryptography.RijndaelManaged, mscorlib";
            default:
                return "System.Security.Cryptography.TripleDESCryptoServiceProvider, mscorlib";
        }
    }

    setEncryptionAlgorithm(value: string) {
        this.encryption.algorithm(value);
    }

    setEncryptionBits(value: number) {
        this.encryption.keyBitsPreference(value);
    }

    setIndexEncryption(value: boolean) {
        this.encryption.encryptIndexes(value);
    }
}

export = resourceCreationModel;
