import database = require("models/resources/database");
import appUrl = require("common/appUrl");

class downloader {
    $downloadFrame = $("#downloadFrame");

    download(db: database, url: string) {
        const dbUrl = appUrl.forDatabaseQuery(db);
        this.$downloadFrame.attr("src", dbUrl + url);
    }

    reset() {
        this.$downloadFrame.attr("src", "");
    }
}

export = downloader
