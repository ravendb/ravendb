import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import messagePublisher = require("common/messagePublisher"); 
import resource = require("models/resources/resource");

class downloader {
    $downloadFrame = $("#downloadFrame");

    download(rs: resource, url: string) {
        new getSingleAuthTokenCommand(rs).execute().done((token: singleAuthToken) => {
            var authToken = (url.indexOf("?") === -1 ? "?" : "&") + "singleUseAuthToken=" + token.Token;
            this.$downloadFrame.attr("src", url + authToken);
        }).fail((qXHR, textStatus, errorThrown) => messagePublisher.reportError("Could not get single auth token for download.", errorThrown));
    }

    reset() {
        this.$downloadFrame.attr("src", "");
    }
}

export = downloader
