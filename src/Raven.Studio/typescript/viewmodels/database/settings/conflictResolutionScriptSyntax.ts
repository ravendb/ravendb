import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");
import { highlight, languages } from "prismjs";

class conflictResolutionScriptSyntax extends dialogViewModelBase {

    view = require("views/database/settings/conflictResolutionScriptSyntax.html");
    
    dialogContainer: Element;
    
    static readonly sampleScript = `// The following variables are available in script context:
// docs - array of conflicted documents
// hasTombstone - true if any of conflicted document is deletion
// resolveToTombstone - return this value if you want to resolve conflict by deleting document 

var maxRecord = 0;
for (var i = 0; i < docs.length; i++) {
    maxRecord = Math.max(docs[i].MaxRecord, maxRecord);   
}
docs[0].MaxRecord = maxRecord;

return docs[0];
     ` ;

    compositionComplete() {
        super.compositionComplete();
        this.bindToCurrentInstance("copySample");
        this.dialogContainer = document.getElementById("conflictResolutionSyntaxDialog");
    }

    copySample() {
        copyToClipboard.copy(conflictResolutionScriptSyntax.sampleScript, "Sample has been copied to clipboard", this.dialogContainer);
    }
    
    scriptHtml = ko.pureComputed(() => {
        return highlight(conflictResolutionScriptSyntax.sampleScript, languages.javascript, "js");
    });

}

export = conflictResolutionScriptSyntax;
