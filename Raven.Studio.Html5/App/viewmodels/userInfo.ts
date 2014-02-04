import getUserInfoCommand = require("commands/getUserInfoCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");

class userInfo extends viewModelBase {

    data = ko.observable<userInfoDto>();
    
    activate(args) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.fetchUserInfo());
        return this.fetchUserInfo();
    }

    fetchUserInfo(): JQueryPromise<userInfoDto> {
        var db = this.activeDatabase();
        if (db) {
            return new getUserInfoCommand(db)
                .execute()
                .done((results: userInfoDto) => this.data(results));
        }

        return null;
    }
}

export = userInfo;