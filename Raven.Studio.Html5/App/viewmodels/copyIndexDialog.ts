import indexDefinition = require("models/indexDefinition");
import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/database");
import getIndexDefinitionCommand = require("commands/getIndexDefinitionCommand");
import saveIndexDefinitionCommand = require("commands/saveIndexDefinitionCommand");
import router = require("plugins/router"); 
import appUrl = require("common/appUrl");
import indexPriority = require("models/indexPriority");
import alertArgs = require("common/alertArgs");
import alertType = require("common/alertType");

class copyIndexDialog extends dialogViewModelBase {
    
    indexJSON = ko.observable<string>("");

    constructor(private indexName: string, private db: database,private isPaste:boolean = false, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);
    }
    
    canActivate(args: any):any {
        if (this.isPaste) {
            return true;
        }
        else{
            var canActivateResult = $.Deferred();
            new getIndexDefinitionCommand(this.indexName, this.db)
                .execute()
                .done((results: indexDefinitionContainerDto) => {
                    var prettifySpacing = 4;
                    this.indexJSON(JSON.stringify(new indexDefinition(results.Index).toDto(), null, prettifySpacing));
                    canActivateResult.resolve({ can: true });
                })
                .fail(() => canActivateResult.reject());
                    canActivateResult.resolve({ can: true });
            return canActivateResult;
        }
    }
    attached() {
        super.attached();
        this.selectText();
    }

    deactivate() {
        $("#indexJSON").unbind('keydown.jwerty');
    }

    selectText() {
        $("#indexJSON").select();
    }


    finishOperation() {
        if (this.isPaste === true && !!this.indexJSON()) {
            var indexDto: indexDefinitionDto;

            try {
                indexDto = JSON.parse(this.indexJSON());
                var testIndex = new indexDefinition(indexDto);
            } catch(e) {
                indexDto = null;
                var title = "Index paste failed, invalid json string";
                ko.postbox.publish("Alert", new alertArgs(alertType.danger, title, e));
                if (console && console.log && typeof console.log === "function") {
                    console.log("Error during command execution", title, e);
                }
            }

            if (indexDto) {

                new getIndexDefinitionCommand(indexDto.Name, this.db)
                    .execute()
                    .fail((request, status, error) => {
                        if (request.status === 404) {
                            new saveIndexDefinitionCommand(indexDto, indexPriority.normal, this.db)
                                .execute()
                                .done(() => {
                                    router.navigate(appUrl.forEditIndex(indexDto.Name, this.db));
                                    this.close();
                                });
                        } else {
                            var title = "Cannot paste Index, Error occured";
                            ko.postbox.publish("Alert", new alertArgs(alertType.danger, title, error));
                        if (console && console.log && typeof console.log === "function") {
                            console.log("Cannot paste Index, Error occured", title, error);
                        }
                        }
                    })
                    .done(() => {
                        var title = "Cannot paste Index, Index with that name already exist";
                        ko.postbox.publish("Alert", new alertArgs(alertType.danger, title));
                        if (console && console.log && typeof console.log === "function") {
                            console.log("Cannot paste Index, Index with that name already exist", title);
                        }
                    });
            } 
        } else {
            this.close();    
        }

        
    }

    close() {
        dialog.close(this);
    }

    activateDocs() {
        this.selectText();
    }

    activateIds() {
        this.selectText();
    }
}

export = copyIndexDialog; 