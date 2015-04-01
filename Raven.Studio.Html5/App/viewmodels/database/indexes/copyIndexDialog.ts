import indexDefinition = require("models/database/index/indexDefinition");
import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import getIndexDefinitionCommand = require("commands/database/index/getIndexDefinitionCommand");
import saveIndexDefinitionCommand = require("commands/database/index/saveIndexDefinitionCommand");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import indexPriority = require("models/database/index/indexPriority");
import messagePublisher = require("common/messagePublisher");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

class copyIndexDialog extends dialogViewModelBase {

    indexJSON = ko.observable("");

    constructor(private indexName: string, private db: database, private isPaste: boolean = false, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);
        aceEditorBindingHandler.install();
    }

    canActivate(args: any): any {
        if (this.isPaste) {
            return true;
        }
        else {
            var canActivateResult = $.Deferred();
            new getIndexDefinitionCommand(this.indexName, this.db)
                .execute()
                .done((results: indexDefinitionContainerDto) => {
                    var prettifySpacing = 4;
                    var jsonString = JSON.stringify(new indexDefinition(results.Index).toDto(), null, prettifySpacing);
                    this.indexJSON(jsonString);
                    canActivateResult.resolve({ can: true });
                })
                .fail(() => canActivateResult.reject());
            canActivateResult.resolve({ can: true });
            return canActivateResult;
        }
    }

    setInitialFocus() {
        // Overrides the base class' setInitialFocus and does nothing.
        // Doing nothing because we will focus the Ace Editor when it's initialized.
    }

    enterKeyPressed(): boolean {
        // Overrides the base class' enterKeyPressed. Because the user might
        // edit the JSON, or even type some in manually, enter might really mean new line, not Save changes.
        if (!this.isPaste) {
            return super.enterKeyPressed();
        }

        return true;
    }

    saveIndex() {
        if (this.isPaste === true && !!this.indexJSON()) {
            var indexDto: indexDefinitionDto;

            try {
                indexDto = JSON.parse(this.indexJSON());
                var testIndex = new indexDefinition(indexDto);
            } catch (e) {
                indexDto = null;
                messagePublisher.reportError("Index paste failed, invalid json string", e);
            }

            if (indexDto) {

                new getIndexDefinitionCommand(indexDto.Name, this.db)
                    .execute()
                    .fail((request, status, error) => {
                        if (request.status === ResponseCodes.NotFound) {
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
        }
    }

    close() {
        dialog.close(this);
    }
}

export = copyIndexDialog; 