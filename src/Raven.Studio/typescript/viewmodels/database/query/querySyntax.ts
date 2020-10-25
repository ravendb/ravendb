import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class querySyntax extends dialogViewModelBase {

    static readonly example0 =
        `from Orders
where Lines.Count > 4
order by Freight as double
select Lines[].ProductName as ProductNames, OrderedAt, ShipTo.City`;

    static readonly example1 =
        `from Orders as o
load o.Company as c
select {
    Name: c.Name.toLowerCase(),
    Country: c.Address.Country,
    LinesCount: o.Lines.length
}`;

    static readonly example2 =
        `from Orders
group by Company
where count() > 5
order by count() desc
select count() as Count, key() as Company
include Company`;

    static readonly example3 =
        `from index 'Orders/Totals' as i
where i.Total > 10000
load i.Company as c
select {
    Name: c.Name,
    Region: c.Address.Region,
    OrderedAt: i.OrderedAt
}`;
    
    copyModel: copyToClipboard;

    compositionComplete() {
        super.compositionComplete();
        
        const htmlElement = document.getElementById("querySyntaxDialog");
        this.copyModel = new copyToClipboard(htmlElement, [
            querySyntax.example0,
            querySyntax.example1,
            querySyntax.example2,
            querySyntax.example3
        ]);
    }

    copyExample(exampleNumber: number) {
        this.copyModel.copyText(exampleNumber, "Example has been copied to clipboard");
    }
}

export = querySyntax;
