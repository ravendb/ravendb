import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import { IndexListItem } from "../../../components/models/indexes";

class bulkIndexOperationConfirm extends confirmViewModelBase<confirmDialogResult> {
    
    view = require("views/database/indexes/bulkIndexOperationConfirm.html");
    
    title: string;
    subTitleHtml: string;
    
    private constructor(private indexes: Array<IndexListItem>, private infinitive: string, private gerund: string, private location: string) {
        super(null);

        this.title = infinitive + " " + this.pluralize(indexes.length, "index", "indexes", true) + "?";
        this.subTitleHtml = indexes.length === 1 ? `You're ${gerund} index:` : `You're ${gerund} <strong>${indexes.length}</strong> indexes:`;
    }
    
    public static forResume(indexes: IndexListItem[], nodeTag: string) {
        return new bulkIndexOperationConfirm(indexes, "Resume", "resuming", "on node " + nodeTag);
    }
    
    public static forPause(indexes: IndexListItem[], nodeTag: string) {
        return new bulkIndexOperationConfirm(indexes, "Pause", "pausing", "on node " + nodeTag);
    }
    
    public static forEnable(indexes: IndexListItem[], nodeTag: string) {
        return new bulkIndexOperationConfirm(indexes, "Enable", "enabling", "on node " + nodeTag);
    }

    public static forClusterWideEnable(indexes: IndexListItem[]) {
        return new bulkIndexOperationConfirm(indexes, "Enable", "enabling", "cluster wide");
    }
    
    public static forDisable(indexes: IndexListItem[], nodeTag: string) {
        return new bulkIndexOperationConfirm(indexes, "Disable", "disabling", "on node " + nodeTag);
    }

    public static forClusterWideDisable(indexes: IndexListItem[]) {
        return new bulkIndexOperationConfirm(indexes, "Disable", "disabling", "cluster wide");
    }
    
}

export = bulkIndexOperationConfirm;
