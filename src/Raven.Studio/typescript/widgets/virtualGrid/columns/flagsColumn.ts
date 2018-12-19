/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import document = require("models/database/documents/document");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");

class flagsColumn implements virtualColumn {
    constructor(protected gridController: virtualGridController<any>) {
    }

    get sortable() {
        return false;
    }

    width = "65px";
    
    header = `<div style="padding-left: 8px;"><i class="icon-flag"></i></div>`;

    get headerAsText() {
        return "Document Flags";
    }

    renderCell(item: document, isSelected: boolean, isSorted: boolean): string {
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
        
        return `<div class="cell text-cell flags-cell ${extraClasses.join(" ")}" style="width: ${this.width}"><i title="Attachments" class="icon-attachment"></i><i title="Revisions" class="icon-revisions"></i><i title="Counters" class="icon-new-counter"></i></div>`;
    }

    toDto(): virtualColumnDto {
        return {
            type: "flags",
            serializedValue: null,
            width: this.width,
            header: null
        }
    }

}

export = flagsColumn;
