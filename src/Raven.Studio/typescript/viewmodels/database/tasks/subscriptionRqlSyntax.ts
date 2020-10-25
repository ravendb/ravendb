import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class subscriptionRqlSyntax extends dialogViewModelBase {

    static readonly example0 =
`from Orders`;

    static readonly example1 =
`from Orders as o
load o.Company as c
select
{
     Name: c.Name.toLowerCase(),
     Country: c.Address.Country,
     LinesCount: o.Lines.length
}`;

    copyModel: copyToClipboard;

    compositionComplete() {
        super.compositionComplete();

        const htmlElement = document.getElementById("subscriptionRqlSyntaxDialog");
        this.copyModel = new copyToClipboard(htmlElement, [
            subscriptionRqlSyntax.example0,
            subscriptionRqlSyntax.example1
        ]);
    }

    copyExample(exampleNumber: number) {
        this.copyModel.copyText(exampleNumber, "Example has been copied to clipboard");
    }
}
export = subscriptionRqlSyntax;
