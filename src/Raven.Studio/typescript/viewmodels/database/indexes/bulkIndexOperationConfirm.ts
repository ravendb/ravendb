import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import index = require("models/database/index/index");

class bulkIndexOperationConfirm extends confirmViewModelBase<confirmDialogResult> {
    title: string;
    subTitleHtml: string;
    
    private constructor(private indexes: Array<index>, private infinitive: string, private gerund: string, private location: string) {
        super(null);

        this.title = infinitive + " " + this.pluralize(indexes.length, "index", "indexes", true) + "?";
        this.subTitleHtml = indexes.length === 1 ? `You're ${gerund} index:` : `You're ${gerund} <strong>${indexes.length}</strong> indexes:`;
    }
    
    public static forResume(indexes: index[], nodeTag: string) {
        return new bulkIndexOperationConfirm(indexes, "Resume", "resuming", "on node " + nodeTag);
    }
    
    public static forPause(indexes: index[], nodeTag: string) {
        return new bulkIndexOperationConfirm(indexes, "Pause", "pausing", "on node " + nodeTag);
    }
    
    public static forEnable(indexes: index[], nodeTag: string) {
        return new bulkIndexOperationConfirm(indexes, "Enable", "enabling", "on node " + nodeTag);
    }

    public static forClusterWideEnable(indexes: index[]) {
        return new bulkIndexOperationConfirm(indexes, "Enable", "enabling", "cluster wide");
    }
    
    public static forDisable(indexes: index[], nodeTag: string) {
        return new bulkIndexOperationConfirm(indexes, "Disable", "disabling", "on node " + nodeTag);
    }

    public static forClusterWideDisable(indexes: index[]) {
        return new bulkIndexOperationConfirm(indexes, "Disable", "disabling", "cluster wide");
    }
    
}

export = bulkIndexOperationConfirm;
