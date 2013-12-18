import getUserInfoCommand = require("commands/getUserInfoCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");

class userInfo {

    data = ko.observable<userInfoDto>();
    
    activate(args) {
        var db = appUrl.getDatabase();
        return new getUserInfoCommand(db)
            .execute()
            .done((results: userInfoDto) => this.data(results));
    }
}

export = userInfo;