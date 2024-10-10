/// <reference path="../../../../typings/tsd.d.ts" />

import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import collection = require("models/database/documents/collection");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import router = require("plugins/router");

class collectionMenuItem implements menuItem {
    type: menuItemType = "collections";
    parent: KnockoutObservable<intermediateMenuItem> = ko.observable(null);

    isOpen(active: KnockoutObservable<menuItem>, coll: collection) {
        return ko.pureComputed(() => {
            const item = active();

            if (!item) {
                return false;
            }
            
            if (item instanceof leafMenuItem) {
                if (item.route === "databases/documents/revisions/bin" && coll.isRevisionsBin) {
                    return true;
                }
                if (item.route === "databases/documents/revisions/all" && coll.isAllRevisions) {
                    return true;
                }

                if (item.route === "databases/documents") {
                    const instruction = router.activeInstruction();
                    if (!instruction || !instruction.params) {
                        return false;
                    }
                    const param0 = instruction.params[0];
                    if (!param0) {
                        return false;
                    }

                    if (coll.isAllDocuments && !param0.collection) {
                        return true;
                    }

                    return param0.collection === coll.name;
                }
            }

            return false;
        });
    }
}

export = collectionMenuItem;
