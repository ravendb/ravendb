import getSingleAuthTokenCommand = require("commands/getSingleAuthTokenCommand");
import messagePublisher = require("common/messagePublisher"); 

import resource = require("models/resource");

class downloader {
    $downloadFrame = $('#downloadFrame');

    download(resource : resource, url : string) {
        new getSingleAuthTokenCommand(resource).execute().done((token: singleAuthToken) => {
            this.$downloadFrame.attr("src", url + "?singleUseAuthToken=" + token.Token);
        }).fail((qXHR, textStatus, errorThrown) => messagePublisher.reportError("Could not get Single Auth Token for download.", errorThrown));
    }

    reset() {
        this.$downloadFrame.attr("src", "");
    }
}

export = downloader
