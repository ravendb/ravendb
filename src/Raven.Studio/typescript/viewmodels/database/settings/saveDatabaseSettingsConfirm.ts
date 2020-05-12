import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import genUtils = require("common/generalUtils");

class saveDatabaseSettingsConfirm extends confirmViewModelBase<confirmDialogResult> {

    settingsToSaveText: string;
    
    constructor(private databaseSettingsToSave: Array<setttingsItem>) {
        super();
        
        if (this.databaseSettingsToSave.length === 0 ) {
            this.settingsToSaveText = "<pre>{ }</pre>";
        } else {
            this.settingsToSaveText = "<pre>{<br>";

            databaseSettingsToSave.forEach(x => {
                this.settingsToSaveText += `   <span>{ ${genUtils.escapeHtml(x.key)} : ${genUtils.escapeHtml(x.value)} }</span><br>`;
            });

            this.settingsToSaveText +=  "}</pre>";
        }
    }

    save() {
        this.confirm();
    }
}

export = saveDatabaseSettingsConfirm;
