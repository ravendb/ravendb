/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import document = require("models/database/documents/document");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");

class flagsColumn implements virtualColumn {
    constructor(protected gridController: virtualGridController<any>) {
    }

    width = "65px";
    
    header = `<div style="padding-left: 8px;">🏳️</div>`; //TODO: change to icon-flags

    get headerAsText() {
        return "Document Flags (🏳️)";
    }

    renderCell(item: document, isSelected: boolean): string {
        const metadata = item.__metadata;
        
        const extraClasses = [] as Array<string>;
        
        if (metadata) {
            const flags = (metadata.flags || "").split(",").map(x => x.trim());
            
            if (_.includes(flags, "HasAttachments")) {
                extraClasses.push("attachments");
            }
            if (_.includes(flags, "HasRevisions")) {
                extraClasses.push("revisions");
            }
            if (_.includes(flags, "HasCounters")) {
                extraClasses.push("counters");
            }
        }
        
        return `<div class="cell text-cell flags-cell ${extraClasses.join(" ")}" style="width: ${this.width}"><i class="icon-attachment"></i><i class="icon-revisions"></i><i class="icon-new-counter"></i></div>`;
    }

}

export = flagsColumn;
