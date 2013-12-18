import getUserInfoCommand = require("commands/getUserInfoCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");

class userInfo {

    data = ko.observable<userInfoDto>();
    activeDbSubscription: KnockoutSubscription;

    constructor() {
        this.activeDbSubscription = ko.postbox.subscribe("ActivateDatabase", (db: database) => this.fetchUserInfo(db));
    }
    
    activate(args) {
        var db = appUrl.getDatabase();
        this.fetchUserInfo(db);
    }

    deactivate() {
        this.activeDbSubscription.dispose();
    }

    fetchUserInfo(db: database) {
        if (db) {
            return new getUserInfoCommand(db)
                .execute()
                .done((results: userInfoDto) => this.data(results));
        }
    }
}

export = userInfo;