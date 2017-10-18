import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class transformationScriptSyntax extends dialogViewModelBase {
    
    etlType = ko.observable<Raven.Client.ServerWide.ETL.EtlType>();
    static readonly sampleScript = `var orderData = {
    Id: id(this),
    OrderLinesCount: this.Lines.length,
    TotalCost: 0
};

for (var i = 0; i < this.Lines.length; i++) {
    var line = this.Lines[i];
    orderData.TotalCost += line.PricePerUnit;
    loadToOrderLines({
        OrderId: id(this),
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.PricePerUnit
    });
}
orderData.TotalCost = Math.round(orderData.TotalCost  * 100) / 100;

loadToOrders(orderData);`;

    scriptHtml = ko.pureComputed(() => {
        return Prism.highlight(transformationScriptSyntax.sampleScript, (Prism.languages as any).javascript);
    });

    constructor(etlType: Raven.Client.ServerWide.ETL.EtlType) {        
        super();
        
        this.etlType(etlType);
    }
}

export = transformationScriptSyntax;
