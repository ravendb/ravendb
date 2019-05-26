import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class conflictResolutionScriptSyntax extends dialogViewModelBase {
    
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

    scriptHtml = ko.pureComputed(() => {
        return Prism.highlight(conflictResolutionScriptSyntax.sampleScript, (Prism.languages as any).javascript);
    });

}

export = conflictResolutionScriptSyntax;
