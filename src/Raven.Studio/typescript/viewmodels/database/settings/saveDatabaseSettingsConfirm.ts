import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import genUtils = require("common/generalUtils");

class saveDatabaseSettingsConfirm extends confirmViewModelBase<confirmDialogResult> {

    settingsToSaveText: string;
    
    constructor(private databaseSettingsToSave: Array<setttingsItem>, private howToReloadDatabaseHtml: string) {
        super();
        
        if (this.databaseSettingsToSave.length === 0) {
            this.settingsToSaveText = "<pre>{ }</pre>";
        } else {
            this.settingsToSaveText = "<pre>{<br>";

            databaseSettingsToSave.forEach(x => {
                const keyPart = `"${genUtils.escapeHtml(x.key)}"`;
                const valuePart =  x.value ? `"${genUtils.escapeHtml(x.value)}"` : "null";
                
                this.settingsToSaveText += `   <span>{ ${keyPart} : ${valuePart} }</span><br>`;
            });

            this.settingsToSaveText +=  "}</pre>";
        }
    }

    save() {
        this.confirm();
    }
}

export = saveDatabaseSettingsConfirm;
