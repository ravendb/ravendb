import indexDefinition = require("models/database/index/indexDefinition");
import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import getIndexDefinitionCommand = require("commands/database/index/getIndexDefinitionCommand");
import saveIndexDefinitionCommand = require("commands/database/index/saveIndexDefinitionCommand");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import copyToClipboard = require("common/copyToClipboard");

class copyIndexDialog extends dialogViewModelBase {

    indexJSON = ko.observable("");
    formattedOnce = false;

    constructor(private indexName: string, private db: database, private isPaste: boolean = false, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);
        aceEditorBindingHandler.install();

        this.indexJSON.subscribe(() => {
            if (this.isPaste === false || this.formattedOnce)
                return;

            this.format();
        });
    }

    canActivate(args: any): any {
        if (this.isPaste) {
            return true;
        } else {
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
        } else {
            this.saveIndex();
        }

        return true;
    }

    format() {
        var newValue = this.indexJSON();

        try {
            var tempIndex = JSON.parse(newValue);
            var formatted = this.stringify(tempIndex);
            this.indexJSON(formatted);
            this.formattedOnce = true;
        } catch (e) {
            //ignore this
        }
    }

    stringify(obj: any) {
        var prettifySpacing = 4;
        return JSON.stringify(obj, null, prettifySpacing);
    }

    saveIndex() {
        if (this.isPaste && this.indexJSON()) {
            var indexDto: indexDefinitionDto;

            try {
                indexDto = JSON.parse(this.indexJSON());
            } catch (e) {
                indexDto = null;
                messagePublisher.reportError("Index paste failed, invalid json string", e);
            }

            if (indexDto) {
                new saveIndexDefinitionCommand(indexDto, this.db)
                    .execute()
                    .done(() => {
                        router.navigate(appUrl.forEditIndex(indexDto.Name, this.db));
                        this.close();
                    });
            }
        } else {
            this.close();
        }
    }

    copyIndex() {
        copyToClipboard.copy(this.indexJSON(), "Copied index to clipboard!");
        this.close();
    }

    close() {
        dialog.close(this);
    }
}

export = copyIndexDialog; 
