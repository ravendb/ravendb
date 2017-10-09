import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class transformationScriptSyntax extends dialogViewModelBase {
    
    etlType = ko.observable<Raven.Client.ServerWide.ETL.EtlType>();
    
    constructor(etlType: Raven.Client.ServerWide.ETL.EtlType) {        
        super();
        
        this.etlType(etlType);
    }
}

export = transformationScriptSyntax;
