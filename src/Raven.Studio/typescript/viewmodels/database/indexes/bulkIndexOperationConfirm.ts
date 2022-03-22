import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import { IndexSharedInfo } from "../../../components/models/indexes";

class bulkIndexOperationConfirm extends confirmViewModelBase<confirmDialogResult> {
    
    view = require("views/database/indexes/bulkIndexOperationConfirm.html");
    
    title: string;
    subTitleHtml: string;
    
    //TODO locations selector
    
    private constructor(private indexes: Array<IndexSharedInfo>, private infinitive: string, private gerund: string) {
        super(null);

        this.title = infinitive + " " + this.pluralize(indexes.length, "index", "indexes", true) + "?";
        this.subTitleHtml = indexes.length === 1 ? `You're ${gerund} index:` : `You're ${gerund} <strong>${indexes.length}</strong> indexes:`;
    }
    
    public static forResume(indexes: IndexSharedInfo[]) {
        return new bulkIndexOperationConfirm(indexes, "Resume", "resuming");
    }
    
    public static forPause(indexes: IndexSharedInfo[]) {
        return new bulkIndexOperationConfirm(indexes, "Pause", "pausing");
    }
    
    public static forEnable(indexes: IndexSharedInfo[]) {
        return new bulkIndexOperationConfirm(indexes, "Enable", "enabling");
    }

    public static forDisable(indexes: IndexSharedInfo[]) {
        return new bulkIndexOperationConfirm(indexes, "Disable", "disabling");
    }
    
}

export = bulkIndexOperationConfirm;
