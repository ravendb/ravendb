import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class mapReduceIndexSyntax extends dialogViewModelBase {

    dialogContainer: Element;

    compositionComplete() {
        super.compositionComplete();
        this.bindToCurrentInstance("copySample");
        this.dialogContainer = document.getElementById("mapReduceIndexSyntaxDialog");
    }

    copySample(sampleTitle: string) {
        const sampleText = [...mapReduceIndexSyntax.linqSamples, ...mapReduceIndexSyntax.javascriptSamples]
            .find(x => x.title === sampleTitle).text;
        
        copyToClipboard.copy(sampleText, "Sample has been copied to clipboard", this.dialogContainer);
    }

    static readonly linqSamples: Array<sampleCode> = [
        {
            title: "Linq map function",
            text:
`from product in docs.Products
where product.PricePerUnit > 10 && product.Discontinued == false
select new {
    product.Supplier,
    product.Category,
    Count = 1,
    Revenue = product.UnitsOnOrder * product.PricePerUnit
}`,
            html:
`<span class="text-info">// Map Function:</span>
<span class="token keyword">from </span>product <span class="token keyword">in </span><span class="token builtin">docs.Products</span>
<span class="token keyword">where </span>product.PricePerUnit <span class="token operator">> </span>10 <span class="token operator">&& </span>product.Discontinued <span class="token operator">== </span>false
<span class="token keyword">select new</span> {
    product.<span class="token string">Supplier</span>,
    product.<span class="token string">Category</span>,
    <span class="token string">Count</span> = 1,
    <span class="token string">Revenue</span> = product.UnitsOnOrder <span class="token operator">* </span>product.PricePerUnit
}


`
        },
        {
            title: "Linq reduce function ",
            text:
`from result in results
group result by new { result.Supplier, result.Category } into g
select new {
   Supplier = g.Key.Supplier,
   Category = g.Key.Category,
   Count = g.Sum(x => x.Count),
   Revenue = g.Sum(x => x.Revenue)
}`,
            html:
`<span class="text-info">// Reduce Function:</span>
<span class="token keyword">from </span>result <span class="token keyword">in </span><span class="token string">results</span>
<span class="token keyword">group </span>result <span class="token keyword">by </span>new { result.Supplier, result.Category } <span class="token keyword">into </span>g
<span class="token keyword">select </span>new {
   <span class="token string">Supplier</span> = g.Key.Supplier,
   <span class="token string">Category</span> = g.Key.Category,
   <span class="token string">Count</span> = g.Sum(x <span class="token operator">=> </span>x.Count),
   <span class="token string">Revenue</span> = g.Sum(x <span class="token operator">=> </span>x.Revenue)
}

`
        }
    ];

    static readonly javascriptSamples: Array<sampleCode> = [
        {
            title: "Javascript map function ",
            text:
`map('Products', (product) => { 
    if (product.PricePerUnit > 10 && product.Discontinued !== false) {
        return { 
            Supplier: product.Supplier, 
            Category: product.Category,
            Count: 1,
            Revenue: product.UnitsOnOrder * product.PricePerUnit
        };
    }
})`,
            html:
`<span class="text-info">// Map Function:</span>
<span class="token keyword">map</span>(<span class="token string">'Products'</span>, (product) <span class="token operator">=></span> { 
    <span class="token keyword">if </span>(product.PricePerUnit <span class="token operator">> </span>10 <span class="token operator">&& </span>product.Discontinued <span class="token operator">!== </span>false) {
        <span class="token keyword">return</span> { 
            Supplier: product.<span class="token string">Supplier</span>, 
            Category: product.<span class="token string">Category</span>,
            <span class="token string">Count</span>: 1,
            <span class="token string">Revenue</span>: product.UnitsOnOrder <span class="token operator">*</span> product.PricePerUnit
        };
    }
})`
        },
        {
            title: "JavaScript reduce function",
            text:
`groupBy(x => ({ Supplier: x.Supplier, Category: x.Category }))
.aggregate(g => { 
    return {
        Supplier: g.key.Supplier,
        Category: g.key.Category,
        Count: g.values.reduce((count, val) => val.Count + count, 0),
        Revenue: g.values.reduce((amount, val) => val.Revenue + amount, 0)
    };
})`,
            html:
`<span class="text-info">// Reduce Function:</span>
<span class="token keyword">groupBy</span>(x <span class="token operator">=></span> ({ Supplier: x.Supplier, Category: x.Category }))
<span class="token keyword">.aggregate</span>(g <span class="token operator">=></span> { 
    <span class="token keyword">return</span> {
        <span class="token string">Supplier</span>: g.key.Supplier,
        <span class="token string">Category</span>: g.key.Category,
        <span class="token string">Count</span>: g.values.reduce((count, val) <span class="token operator">=></span> val.Count <span class="token operator">+</span> count, 0),
        <span class="token string">Revenue</span>: g.values.reduce((amount, val) <span class="token operator">=></span> val.Revenue <span class="token operator">+</span> amount, 0)
    };
})`
        }
    ];
}

export = mapReduceIndexSyntax;
