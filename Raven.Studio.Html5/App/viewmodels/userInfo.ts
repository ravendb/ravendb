import getUserInfoCommand = require("commands/getUserInfoCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import activeDbViewModelBase = require("viewmodels/activeDbViewModelBase");

class userInfo extends activeDbViewModelBase {

    data = ko.observable<userInfoDto>();
    
    activate(args) {
        super.activate(args);
        this.activeDatabase.subscribe(() => this.fetchUserInfo());
        this.fetchUserInfo();
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