import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class subscriptionRqlSyntax extends dialogViewModelBase {

    view = require("views/database/tasks/subscriptionRqlSyntax.html");
    
    dialogContainer: Element;

    compositionComplete() {
        super.compositionComplete();
        this.bindToCurrentInstance("copySample");
        this.dialogContainer = document.getElementById("subscriptionRqlSyntaxDialog");
    }

    copySample(sampleTitle: string) {
        const sampleText = subscriptionRqlSyntax.samples.find(x => x.title === sampleTitle).text;
        copyToClipboard.copy(sampleText, "Sample has been copied to clipboard", this.dialogContainer);
    }

    static readonly samples: Array<sampleCode> = [
        {
            title: "A subscription for all documents in a collection:",
            text: 
`from Orders`,
            html:
`<span class="string">// This query returns ALL documents from the Orders collection.</span>
<span class="token keyword">from</span><span class="token string"> Orders</span>`
        }, 
        {
            title: "A subscription for projected (partial) document data:",
            text:
`from Orders as o
load o.Company as c
select
{
     Name: c.Name.toLowerCase(),
     Country: c.Address.Country,
     LinesCount: o.Lines.length
}`,
            html:
`<span class="string">// This query projects selected fields from all documents in the<br/>// Orders collection.</span>
<span class="string">// Also loads the related Company document.</span>
<span class="token keyword">from</span><span class="token string"> Orders </span><span class="token keyword">as</span> o
<span class="token keyword">load</span> o.Company <span class="token keyword">as</span> c
<span class="token keyword">select</span>
<span class="token punctuation">{</span>
     Name: c.Name.toLowerCase(),
     Country: c.Address.Country,
     LinesCount: o.Lines.length
<span class="token punctuation">}</span>`
        },       
        {
            title: "A subscription that filters documents:",
            text:
`declare function getOrderLinesSum(doc) {
    var sum = 0;
    for (var i in doc.Lines) {
        sum += doc.Lines[i].PricePerUnit * doc.Lines[i].Quantity;
    }
    return sum;
}
 
From Orders as o 
Where getOrderLinesSum(o) > 100
`,
            html:
`<span class="string">// Returns only documents where the total order value exceeds 100.</span>
<span class="token keyword">declare</span> function getOrderLinesSum(doc) <span class="token punctuation">{</span>
    <span class="token keyword">var</span> sum = 0;
    <span class="token keyword">for</span> (<span class="token keyword">var</span> i <span class="token keyword">in</span> doc.Lines) <span class="token punctuation">{</span>
        sum += doc.Lines[i].PricePerUnit * doc.Lines[i].Quantity;
    <span class="token punctuation">}</span>
    <span class="token keyword">return</span> sum;
<span class="token punctuation">}</span>
<span class="token keyword">from</span><span class="token string"> Orders </span><span class="token keyword">as</span> o
<span class="token keyword">where</span> getOrderLinesSum(o) &gt; 100`
        },
        {
            title: "A subscription for document revisions:",
            text:
`From Orders (Revisions = true)`,
            html:
`<span class="string">// Returns all document revisions in the Orders collection.</span>
<span class="string">// Note: Revisions must be enabled for the collection.</span>
<span class="token keyword">from</span><span class="token string"> Orders</span><span class="token punctuation">(</span><span class="token keyword">Revisions</span> = <span class="token boolean">true</span><span class="token punctuation">)</span>`
        }
    ];
}

export = subscriptionRqlSyntax;
