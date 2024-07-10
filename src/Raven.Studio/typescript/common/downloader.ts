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
    
    static fillHiddenFields(object: any, targetForm: JQuery) {
        targetForm.empty();
        
        const addField = (key: string, value: any) => {
            if (typeof value === "undefined") {
                return;
            }
            const $input = $("<input />")
                .attr("type", "hidden")
                .attr("name", key)
                .val(value);
            
            targetForm.append($input);
        }
        
        Object.keys(object).forEach(key => {
            const value = object[key];
            
            if (Array.isArray(value)) {
                value.forEach(v => addField(key, v));
            } else {
                addField(key, value);
            }
        });
    }
}

export = downloader
