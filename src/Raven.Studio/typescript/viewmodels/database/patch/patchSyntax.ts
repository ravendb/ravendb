import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class patchSyntax extends dialogViewModelBase {

    static readonly example0 =
`from Orders 
update {
    this.Lines = this.Lines.filter(l => l.Product != 'products/1');
}`;

    static readonly example1 =
`from Orders as o
load o.Company as c
update {
    o.CompanyName = c.Name;
}`;

    static readonly example2 =
`from index 'Orders/Totals' as i
where i.Total > 10000
load i.Company as c
update { 
    i.LowerName = c.Name.toLowerCase();
}`;

    static readonly example3 =
`from Orders 
update {
    this.DocumentId = id(this);
    this.DocumentCollection = this["@metadata"]["@collection"];
}`;

    static readonly example4 =
`from Persons
update {
    timeseries("Persons/1", "HeartRate").append("2020-06-25T10:48:14.794", [120, 80], "TagName");
}`;

    copyModel: copyToClipboard;

    compositionComplete() {
        super.compositionComplete();

        const htmlElement = document.getElementById("patchSyntaxDialog");
        this.copyModel = new copyToClipboard(htmlElement, [
            patchSyntax.example0,
            patchSyntax.example1,
            patchSyntax.example2,
            patchSyntax.example3,
            patchSyntax.example4
        ]);
    }

    copyExample(exampleNumber: number) {
        this.copyModel.copyText(exampleNumber, "Example has been copied to clipboard");
    }
}
export = patchSyntax;
