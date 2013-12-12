import http = require("plugins/http");
import app = require("durandal/app");
import sys = require("durandal/system");
import router = require("plugins/router");

import collection = require("models/collection");
import database = require("models/database");
import document = require("models/document");
import deleteCollection = require("viewmodels/deleteCollection");
import raven = require("common/raven");
import pagedList = require("common/pagedList");

class userInfo {

    displayName = "user info";
    data = ko.observable();

    ravenDb: raven;

    constructor() {
        this.ravenDb = new raven();
    }

	activate(args) {
		console.log("this is USERINFO!");

        if (args && args.database) {
            ko.postbox.publish("ActivateDatabaseWithName", args.database);
        }

        this.ravenDb.userInfo()
            .done(info => {
                this.data(info);
            });
    }

    canDeactivate() {
        return true;
    }
}

export = userInfo;