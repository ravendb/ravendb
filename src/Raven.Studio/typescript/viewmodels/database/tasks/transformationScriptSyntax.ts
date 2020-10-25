import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class transformationScriptSyntax extends dialogViewModelBase {

    etlType = ko.observable<Raven.Client.Documents.Operations.ETL.EtlType>();
    copyModel: copyToClipboard;

    static readonly ravenEtlExample0 =
`loadToNewEmployees ({
        Name: this.LastName + " some new data..",
        Title: this.Title + " some new data.."
});`;

    static readonly ravenEtlExample1 =
`loadToUsers ({
        Name: this.Name + " some new data.."
});

function loadCountersOfUsersBehavior(documentId, counterName) {
    return true;
}`;

    static readonly ravenEtlExample2 =
`loadToUsers ({
        Name: this.Name + " some new data.."
});

function deleteDocumentsOfUsersBehavior(documentId) {
    return false; // don't propagate document deletions
}`;
    
    static readonly sampleScriptForSql = `var orderData = {
    Id: id(this),
    OrderLinesCount: this.Lines.length,
    TotalCost: 0
};

for (var i = 0; i < this.Lines.length; i++) {
    var line = this.Lines[i];
    orderData.TotalCost += line.PricePerUnit;
    
    // Load to SQL table 'OrderLines'
    loadToOrderLines({
        OrderId: id(this),
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.PricePerUnit
    });
}
orderData.TotalCost = Math.round(orderData.TotalCost * 100) / 100;

// Load to SQL table 'Orders'
loadToOrders(orderData);`;

    sqlScriptHtml = ko.pureComputed(() => {
        return Prism.highlight(transformationScriptSyntax.sampleScriptForSql, (Prism.languages as any).javascript);
    });

    constructor(etlType: Raven.Client.Documents.Operations.ETL.EtlType) {
        super();
        
        this.etlType(etlType);
    }

    compositionComplete() {
        super.compositionComplete();

        const htmlElement = document.getElementById("transformationScriptSyntaxDialog");
        this.copyModel = new copyToClipboard(htmlElement, [
            transformationScriptSyntax.ravenEtlExample0,
            transformationScriptSyntax.ravenEtlExample1,
            transformationScriptSyntax.ravenEtlExample2,
            transformationScriptSyntax.sampleScriptForSql
        ]);
    }

    copyExample(exampleNumber: number) {
        this.copyModel.copyText(exampleNumber, "Example has been copied to clipboard");
    }
}

export = transformationScriptSyntax;
