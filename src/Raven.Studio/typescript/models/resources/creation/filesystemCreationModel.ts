/// <reference path="../../../../typings/tsd.d.ts"/>

import resource = require("models/resources/resource");
import license = require("models/auth/license");
import resourceCreationModel = require("models/resources/creation/resourceCreationModel");
import configuration = require("configuration");

class filesystemCreationModel extends resourceCreationModel {

    get resourceType() {
        return "file system";
    } 

    toDto(): Raven.Abstractions.Data.DatabaseDocument { //TODO: update returned type
        const settings: dictionary<string> = {};
        const securedSettings: dictionary<string> = {};

        /* TODO:
        var settings: dictionary<string> = {
            "Raven/ActiveBundles": bundles.join(";")
        }

        settings["Raven/FileSystem/DataDir"] = (!this.isEmptyStringOrWhitespace(fileSystemPath)) ? fileSystemPath : "~\\FileSystems\\" + fileSystemName;
        if (storageEngine) {
            settings["Raven/FileSystem/Storage"] = storageEngine;
        }
        if (!this.isEmptyStringOrWhitespace(filesystemLogs)) {
            settings["Raven/TransactionJournalsPath"] = filesystemLogs;
        }
        if (!this.isEmptyStringOrWhitespace(tempPath)) {
            settings["Raven/Voron/TempPath"] = tempPath;
        }*/

        this.fillEncryptionSettingsIfNeeded(securedSettings);

        return {
            Id: this.name(),
            Settings: settings,
            SecuredSettings: securedSettings,
            Disabled: false
        };
    }
    
}

export = filesystemCreationModel;
