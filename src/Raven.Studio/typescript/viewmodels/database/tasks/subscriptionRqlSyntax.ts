import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class subscriptionRqlSyntax extends dialogViewModelBase {

    htmlElement: HTMLElement;

    compositionComplete() {
        super.compositionComplete();
        this.bindToCurrentInstance("copySample");
        this.htmlElement = document.getElementById("subscriptionRqlSyntaxDialog");
    }

    copySample(sampleTitle: string) {
        const sampleText = subscriptionRqlSyntax.samples.find(x => x.title === sampleTitle).text;
        copyToClipboard.copy(sampleText, "Sample has been copied to clipboard", this.htmlElement);
    }

    static readonly samples: Array<sampleCode> = [
        {
            title: "Entire Collection Documents",
            text: 
`from Orders`,
            html:
`<span class="token keyword">from</span><span class="token string"> Orders</span>`
        }, 
        {
            title: "Collection Documents with Projection",
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
`<span class="token keyword">from</span><span class="token string"> Orders </span><span class="token keyword">as</span> o
<span class="token keyword">load</span> o.Company <span class="token keyword">as</span> c
<span class="token keyword">select</span>
<span class="token punctuation">{</span>
     Name: c.Name.toLowerCase(),
     Country: c.Address.Country,
     LinesCount: o.Lines.length
<span class="token punctuation">}</span>`
        }
    ];
}

export = subscriptionRqlSyntax;
