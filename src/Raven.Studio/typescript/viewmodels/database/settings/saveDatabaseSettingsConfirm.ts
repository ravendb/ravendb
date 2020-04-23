import confirmViewModelBase = require("viewmodels/confirmViewModelBase");

class saveDatabaseSettingsConfirm extends confirmViewModelBase<confirmDialogResult> {

    settingsToSaveText: string;
    
    constructor(private databaseSettingsToSave: Array<setttingsItem>) {
        super();
        
        if (this.databaseSettingsToSave.length === 0 ) {
            this.settingsToSaveText = "<div class='bg-info text-info padding padding-sm'>" +
                                          "<span >No configuration keys have been overriden.<br>" +
                                          "Upon save, values for the database configuration keys will taken from either the <strong>default configuration</strong>,<br>" +
                                          "Or from the <strong>server configuration</strong>, if defined.</span>" +
                                      "</div>";
            
        } else {
            this.settingsToSaveText = "<pre>";

            databaseSettingsToSave.forEach(x => {
                this.settingsToSaveText += `<span>{ ${x.key} : ${x.value} }</span><br>`;
            });

            this.settingsToSaveText +=  "</pre>";
        }
    }

    save() {
        this.confirm();
    }
}

export = saveDatabaseSettingsConfirm;
