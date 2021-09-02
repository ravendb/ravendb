import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import viewModelBase = require("viewmodels/viewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class querySyntax extends dialogViewModelBase {

    dialogContainer: Element;
    clientVersion = viewModelBase.clientVersion;

    compositionComplete() {
        super.compositionComplete();
        this.bindToCurrentInstance("copySample");
        this.dialogContainer = document.getElementById("querySyntaxDialog");
    }

    copySample(sampleTitle: string) {
        const sampleText = querySyntax.samples.find(x => x.title === sampleTitle).text;
        copyToClipboard.copy(sampleText, "Sample has been copied to clipboard", this.dialogContainer);
    }

    static readonly samples: Array<sampleCode> = [
        { 
            title: "Simple collection query",
            text:
`from Orders
where Lines.Count > 4
order by Freight as double
select Lines[].ProductName as ProductNames, OrderedAt, ShipTo.City`,
            html: 
`<span class="token keyword">from</span> <span class="token string">Orders</span>
<span class="token keyword">where</span> Lines.Count <span class="token operator">></span> <span class="token number">4</span>
<span class="token keyword">order by</span> Freight <span class="token keyword">as double</span>
<span class="token keyword">select</span> Lines[].ProductName <span class="token keyword">as</span> ProductNames, OrderedAt, ShipTo.City`
        },
        {
            title: "Using JavaScript select",
            text:
`from Orders as o
load o.Company as c
select {
    Name: c.Name.toLowerCase(),
    Country: c.Address.Country,
    LinesCount: o.Lines.length
}`,
            html:
`<span class="token keyword">from</span> <span class="token string">Orders</span> <span class="token keyword">as</span> o
<span class="token keyword">load</span> o.Company <span class="token keyword">as</span> c
<span class="token keyword">select</span> <span class="token punctuation">{</span>
    Name: c.Name.toLowerCase(),
    Country: c.Address.Country,
    LinesCount: o.Lines.length
<span class="token punctuation">}</span>`
        },
        {
            title: "Group by",
            text:
`from Orders
group by Company
where count() > 5
order by count() desc
select count() as Count, key() as Companyd
include Company`,
            html:
`<span class="token keyword">from</span> <span class="token string">Orders</span>
<span class="token keyword">group by</span> Company
<span class="token keyword">where</span> <span class="token builtin">count()</span> <span class="token operator">></span> <span class="token number">5</span>
<span class="token keyword">order by</span> <span class="token builtin">count()</span> <span class="token keyword">desc</span>
<span class="token keyword">select</span> <span class="token builtin">count()</span> <span class="token keyword">as</span> Count, <span class="token builtin">key()</span> <span class="token keyword">as</span> Company
<span class="token keyword">include</span> Company`
        },
        {
            title: "Querying an index",
            text:
`from index 'Orders/Totals' as i
where i.Total > 10000
load i.Company as c
select {
    Name: c.Name,
    Region: c.Address.Region,
    OrderedAt: i.OrderedAt
}`,
            html:
`<span class="token keyword">from index</span> <span class="token string">'Orders/Totals'</span> <span class="token keyword">as</span> i
<span class="token keyword">where</span> i.Total <span class="token operator">></span> <span class="token number">10000</span>
<span class="token keyword">load</span> i.Company <span class="token keyword">as</span> c
<span class="token keyword">select</span> <span class="token punctuation">{</span>
    Name: c.Name,
    Region: c.Address.Region,
    OrderedAt: i.OrderedAt
<span class="token punctuation">}</span>`
        }
    ];
}

export = querySyntax;
