import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import messagePublisher = require("common/messagePublisher"); 
import database = require("models/resources/database");

class downloader {
    $downloadFrame = $("#downloadFrame");

    download(db: database, url: string) {
        new getSingleAuthTokenCommand(db).execute().done((token: singleAuthToken) => {
            var authToken = (url.indexOf("?") === -1 ? "?" : "&") + "singleUseAuthToken=" + token.Token;
            this.$downloadFrame.attr("src", url + authToken);
        }).fail((qXHR, textStatus, errorThrown) => messagePublisher.reportError("Could not get single auth token for download.", errorThrown));
    }

    reset() {
        this.$downloadFrame.attr("src", "");
    }
}

export = downloader
