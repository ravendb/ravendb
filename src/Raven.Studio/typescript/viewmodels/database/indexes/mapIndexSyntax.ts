import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class mapIndexSyntax extends dialogViewModelBase {

    static readonly example0 =
`from company in docs.Companies
where company.Address.Country == "USA"
select new {
     company.Name,
     company.Phone,
     City = company.Address.City
}`;

    static readonly example1 =
`map("Companies", (company) => {
    if (company.Address.Country === "USA") {
        return {
            Name: company.Name,
            Phone: company.Phone,
            City: company.Address.City
        };
    }
})`;

    copyModel: copyToClipboard;

    compositionComplete() {
        super.compositionComplete();

        const htmlElement = document.getElementById("mapIndexSyntaxDialog");
        this.copyModel = new copyToClipboard(htmlElement, [
            mapIndexSyntax.example0,
            mapIndexSyntax.example1
        ]);
    }

    copyExample(exampleNumber: number) {
        this.copyModel.copyText(exampleNumber, "Example has been copied to clipboard");
    }
}
export = mapIndexSyntax;
