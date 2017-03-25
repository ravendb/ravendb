import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import messagePublisher = require("common/messagePublisher"); 
import database = require("models/resources/database");
import appUrl = require("common/appUrl");

class downloader {
    $downloadFrame = $("#downloadFrame");

    download(db: database, url: string) {
        const dbUrl = appUrl.forDatabaseQuery(db);
        new getSingleAuthTokenCommand(db).execute().done((token: singleAuthToken) => {
            var authToken = (url.indexOf("?") === -1 ? "?" : "&") + "singleUseAuthToken=" + token.Token;
            this.$downloadFrame.attr("src", dbUrl + url + authToken);
        }).fail((qXHR, textStatus, errorThrown) => messagePublisher.reportError("Could not get single auth token for download.", errorThrown));
    }

    reset() {
        this.$downloadFrame.attr("src", "");
    }
}

export = downloader
