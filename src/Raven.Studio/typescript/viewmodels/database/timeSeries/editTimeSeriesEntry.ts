import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import timeSeriesModel = require("models/database/timeSeries/timeSeriesModel");
import saveTimeSeriesCommand = require("../../../commands/database/documents/timeSeries/saveTimeSeriesCommand");

class editTimeSeriesEntry extends dialogViewModelBase {

    spinners = {
        save: ko.observable<boolean>(false)
    };
    
    model = ko.observable<timeSeriesModel>();
    
    lockSeriesName: boolean;
    lockTimeStamp: boolean;
    
    constructor(private documentId: string, private db: database, private timeSeriesName: string, private editDto?: Raven.Client.Documents.Session.TimeSeriesValue) {
        super();
        
        this.lockTimeStamp = !!editDto;
        this.lockSeriesName = !!this.timeSeriesName;
        
        const model = editDto 
            ? new timeSeriesModel(timeSeriesName, editDto) 
            : timeSeriesModel.empty(timeSeriesName || undefined);
        
        this.model(model);
    }
    
    save() {
        if (!this.isValid(this.model().validationGroup)) {
            return false;
        }
        
        this.spinners.save(true);
        
        const dto = this.model().toDto();
        
        new saveTimeSeriesCommand(this.documentId, dto, this.db)
            .execute()
            .done(() => {
                dialog.close(this, this.model().name());
            })
            .always(() => this.spinners.save(false));
    }

    cancel() {
        dialog.close(this, null);
    }

    deactivate() {
        super.deactivate(null);
    }
}

export = editTimeSeriesEntry;
