import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");
import viewModelBase = require("viewmodels/viewModelBase");

class patchSyntax extends dialogViewModelBase {

    dialogContainer: Element;
    clientVersion = viewModelBase.clientVersion;

    compositionComplete() {
        super.compositionComplete();
        this.bindToCurrentInstance("copySample");
        this.dialogContainer = document.getElementById("patchSyntaxDialog");
    }

    copySample(sampleTitle: string) {
        const sampleText = patchSyntax.samples.find(x => x.title === sampleTitle).text;
        copyToClipboard.copy(sampleText, "Sample has been copied to clipboard", this.dialogContainer);
    }

    static readonly samples: Array<sampleCode> = [
        {
            title: "Filter out an array item",
            text:
`from Orders 
update {
    this.Lines = this.Lines.filter(l => l.Product != 'products/1');
}`,
            html:
`<span class="token keyword">from</span><span class="token string"> Orders </span>
<span class="token keyword">update</span><span class="token punctuation"> { </span>
    this.Lines = this.Lines.filter(l => l.Product <span class="token operator">!=</span> <span class="token string">'products/1'</span>);
<span class="token punctuation">}</span>`
        },
        {
            title: "Denormalize the company name",
            text:
`from Orders as o
load o.Company as c
update {
    o.CompanyName = c.Name;
}`,
            html:
`<span class="token keyword">from</span> <span class="token string">Orders</span> <span class="token keyword">as</span> o
<span class="token keyword">load</span> o.Company <span class="token keyword">as</span> c
<span class="token keyword">update</span><span class="token punctuation"> { </span>
    o.CompanyName = c.Name;
<span class="token punctuation">}</span>`
        },
        {
            title: "Use JavaScript to patch",
            text:
`from index 'Orders/Totals' as i
where i.Total > 10000
load i.Company as c
update { 
    i.LowerName = c.Name.toLowerCase();
}`,
            html:
`<span class="token keyword">from index</span><span class="token string"> 'Orders/Totals' </span><span class="token keyword">as</span> i
<span class="token keyword">where</span> i.Total<span class="token operator"> > </span>10000
<span class="token keyword">load</span> i.Company <span class="token keyword">as</span> c
<span class="token keyword">update</span><span class="token punctuation"> { </span>
    i.LowerName = c.Name.toLowerCase();
<span class="token punctuation">}</span>`
        },
        {
            title: "Access the metadata",
            text:
`from Orders 
update {
    this.DocumentId = id(this);
    this.DocumentCollection = this["@metadata"]["@collection"];
}`,
            html:
`<span class="token keyword">from </span><span class="token string">Orders </span>
<span class="token keyword">update</span><span class="token punctuation"> { </span>
    <span class="token keyword">this</span>.DocumentId = id(<span class="token keyword">this</span>);
    <span class="token keyword">this</span>.DocumentCollection = <span class="token keyword">this</span>["@metadata"]["@collection"];
<span class="token punctuation">}</span>`
        },
        {
            title: "Add a time series entry",
            text:
`from Persons
update {
    timeseries("Persons/1", "HeartRate").append("2020-06-25T10:48:14.794", [120, 80], "TagName");
}`,
            html:
`<span class="token keyword">from </span><span class="token string">Persons</span>
<span class="token keyword">update</span><span class="token punctuation"> { </span>
    timeseries(<span class="token string">"Persons/1"</span>, <span class="token string">"HeartRate"</span>).append(<span class="token string">"2020-06-25T10:48:14.794"</span>, [120, 80], <span class="token string">"TagName"</span>);
<span class="token punctuation">}</span>`
        },
    ];
}

export = patchSyntax;
