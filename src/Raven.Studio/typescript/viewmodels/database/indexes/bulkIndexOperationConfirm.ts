import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import { IndexSharedInfo } from "../../../components/models/indexes";
import { MultipleDatabaseLocationSelector } from "../../../components/common/MultipleDatabaseLocationSelector";

interface bulkIndexOperationConfirmResult extends confirmDialogResult {
    locations: databaseLocationSpecifier[];
}

class bulkIndexOperationConfirm extends confirmViewModelBase<bulkIndexOperationConfirmResult> {
    
    view = require("views/database/indexes/bulkIndexOperationConfirm.html");

    readonly locations: databaseLocationSpecifier[];
    
    selectedLocations = ko.observableArray<databaseLocationSpecifier>([]);
    
    title: string;
    subTitleHtml: string;
    
    locationSelectorOptions: ReactInKnockout<typeof MultipleDatabaseLocationSelector> = ko.pureComputed(() => ({
        component: MultipleDatabaseLocationSelector,
        props: {
            locations: this.locations,
            selectedLocations: this.selectedLocations(),
            setSelectedLocations: l => this.selectedLocations(l)
        } as Parameters<typeof MultipleDatabaseLocationSelector>[0]
    }))

    protected prepareResponse(can: boolean): bulkIndexOperationConfirmResult {
        return {
            can: can,
            locations: this.selectedLocations()
        };
    }
    
    indexes: Array<IndexSharedInfo>;

    infinitive: string;

    private constructor(indexes: Array<IndexSharedInfo>, infinitive: string, private gerund: string, locations: databaseLocationSpecifier[]) {
        super(null);
        this.infinitive = infinitive;
        this.indexes = indexes;

        this.locations = locations;
        
        this.selectedLocations(locations);
        
        this.title = infinitive + " " + this.pluralize(indexes.length, "index", "indexes", true) + "?";
        this.subTitleHtml = indexes.length === 1 ? `You're ${gerund} index:` : `You're ${gerund} <strong>${indexes.length}</strong> indexes:`;
    }
    
    public static forResume(indexes: IndexSharedInfo[], locations: databaseLocationSpecifier[]) {
        return new bulkIndexOperationConfirm(indexes, "Resume", "resuming", locations);
    }
    
    public static forPause(indexes: IndexSharedInfo[], locations: databaseLocationSpecifier[]) {
        return new bulkIndexOperationConfirm(indexes, "Pause", "pausing", locations);
    }
    
    public static forEnable(indexes: IndexSharedInfo[], locations: databaseLocationSpecifier[]) {
        return new bulkIndexOperationConfirm(indexes, "Enable", "enabling", locations);
    }

    public static forDisable(indexes: IndexSharedInfo[], locations: databaseLocationSpecifier[]) {
        return new bulkIndexOperationConfirm(indexes, "Disable", "disabling", locations);
    }
    
}

export = bulkIndexOperationConfirm;
