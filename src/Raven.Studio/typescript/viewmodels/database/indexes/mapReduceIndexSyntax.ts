import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class mapReduceIndexSyntax extends dialogViewModelBase {

    static readonly example0 =
`from product in docs.Products
where product.PricePerUnit > 10 && product.Discontinued == false
select new {
    product.Supplier,
    product.Category,
    Count = 1,
    Revenue = product.UnitsOnOrder * product.PricePerUnit
}`;

    static readonly example1 =
`from result in results
group result by new { result.Supplier, result.Category } into g
select new {
   Supplier = g.Key.Supplier,
   Category = g.Key.Category,
   Count = g.Sum(x => x.Count),
   Revenue = g.Sum(x => x.Revenue)
}`;

    static readonly example2 =
`map('Products', (product) => { 
    if (product.PricePerUnit > 10 && product.Discontinued !== false) {
        return { 
            Supplier: product.Supplier, 
            Category: product.Category,
            Count: 1,
            Revenue: product.UnitsOnOrder * product.PricePerUnit
        };
    }
})`;

    static readonly example3 =
`groupBy(x => ({ Supplier: x.Supplier, Category: x.Category }))
.aggregate(g => { 
    return {
        Supplier: g.key.Supplier,
        Category: g.key.Category,
        Count: g.values.reduce((count, val) => val.Count + count, 0),
        Revenue: g.values.reduce((amount, val) => val.Revenue + amount, 0)
    };
})`;

    copyModel: copyToClipboard;

    compositionComplete() {
        super.compositionComplete();

        const htmlElement = document.getElementById("mapReduceIndexSyntaxDialog");
        this.copyModel = new copyToClipboard(htmlElement, [
            mapReduceIndexSyntax.example0,
            mapReduceIndexSyntax.example1,
            mapReduceIndexSyntax.example2,
            mapReduceIndexSyntax.example3
        ]);
    }

    copyExample(exampleNumber: number) {
        this.copyModel.copyText(exampleNumber, "Example has been copied to clipboard");
    }
}
export = mapReduceIndexSyntax;
