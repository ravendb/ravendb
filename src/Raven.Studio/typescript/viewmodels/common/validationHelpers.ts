import verifyDocumentsIDsCommand = require("commands/database/documents/verifyDocumentsIDsCommand");
import database = require("models/resources/database");
import generalUtils = require("common/generalUtils");

class validationHelpers {

    static addDocumentIdValidation(field: KnockoutObservable<string>, db: database, onlyIf?: () => boolean) {
        if (!onlyIf)
            onlyIf = () => true;
        
        const verifyDocuments = (val: string, params: any, callback: (currentValue: string, result: boolean) => void) => {
            new verifyDocumentsIDsCommand([val], db)
                .execute()
                .done((ids: string[]) => {
                    callback(field(), ids.length > 0);
                });
        };

        field.extend({
            required: true,
            validation: {
                message: "Document doesn't exist.",
                async: true,
                onlyIf: onlyIf,
                validator: generalUtils.debounceAndFunnel(verifyDocuments)
            }
        });
    }

}

export = validationHelpers
