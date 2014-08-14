import indexDefinition = require("models/indexDefinition");
import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/database");
import getIndexMergeSuggestionsCommand = require("commands/getIndexMergeSuggestionsCommand");
import saveIndexDefinitionCommand = require("commands/saveIndexDefinitionCommand");
/*import router = require("plugins/router"); 
import appUrl = require("common/appUrl");
import indexPriority = require("models/indexPriority");
import messagePublisher = require("common/messagePublisher");*/

class indexMergeSuggestionsDialog extends dialogViewModelBase {
    
    indexJSON = ko.observable<string>("");

    constructor(private db: database, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);
    }
    
    canActivate(args: any) :any {
        var canActivateResult = $.Deferred();
        new getIndexMergeSuggestionsCommand(this.db)
            .execute()
            .done((results: indexMergeSuggestionsDto) => {
/*                var prettifySpacing = 4;
                this.indexJSON(JSON.stringify(new indexDefinition(results.Index).toDto(), null, prettifySpacing));*/
                canActivateResult.resolve({ can: true });
            })
            .fail(() => canActivateResult.reject());
                //canActivateResult.resolve({ can: true });
        return canActivateResult;
    }

    attached() {
        super.attached();
        
    }

    deactivate() {
        
    }

    selectText() {
        
    }

    saveIndex() {
/*        if (this.isPaste === true && !!this.indexJSON()) {
            var indexDto: indexDefinitionDto;

            try {
                indexDto = JSON.parse(this.indexJSON());
                var testIndex = new indexDefinition(indexDto);
            } catch(e) {
                indexDto = null;
                messagePublisher.reportError("Index paste failed, invalid json string", e);
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
                            messagePublisher.reportError("Cannot paste index, error occured!", error);
                        }
                    })
                    .done(() => messagePublisher.reportError("Cannot paste index, error occured!", "Index with that name already exists!"));
            } 
        } else {
            this.close();    
        }*/
    }

    close() {
        dialog.close(this);
    }

/*    activateDocs() {
        this.selectText();
    }

    activateIds() {
        this.selectText();
    }*/
}

export = indexMergeSuggestionsDialog; 