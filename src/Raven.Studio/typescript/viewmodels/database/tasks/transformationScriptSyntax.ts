import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class transformationScriptSyntax extends dialogViewModelBase {
    
    etlType = ko.observable<Raven.Client.Documents.Operations.ETL.EtlType>();
    static readonly sampleScript = `var orderData = {
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
orderData.TotalCost = Math.round(orderData.TotalCost  * 100) / 100;

// Load to SQL table 'Orders'
loadToOrders(orderData);`;

    scriptHtml = ko.pureComputed(() => {
        return Prism.highlight(transformationScriptSyntax.sampleScript, (Prism.languages as any).javascript);
    });

    constructor(etlType: Raven.Client.Documents.Operations.ETL.EtlType) {        
        super();
        
        this.etlType(etlType);
    }
}

export = transformationScriptSyntax;
