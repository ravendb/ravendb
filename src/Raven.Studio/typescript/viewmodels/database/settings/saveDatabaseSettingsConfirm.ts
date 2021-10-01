import confirmViewModelBase = require("viewmodels/confirmViewModelBase");

class saveDatabaseSettingsConfirm extends confirmViewModelBase<confirmDialogResult> {

    view = require("views/database/settings/saveDatabaseSettingsConfirm.html");

    settingsToSaveText: string;
    
    constructor(private databaseSettingsToSave: object, private howToReloadDatabaseHtml: string) {
        super();

        const settingsJson = JSON.stringify(databaseSettingsToSave, null, 4);
        const settingsHtml = Prism.highlight(settingsJson, (Prism.languages as any).javascript)

        this.settingsToSaveText = settingsHtml;
    }

    save() {
        this.confirm();
    }
}

export = saveDatabaseSettingsConfirm;
