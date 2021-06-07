import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class mapIndexSyntax extends dialogViewModelBase {

    dialogContainer: Element;

    compositionComplete() {
        super.compositionComplete();
        this.bindToCurrentInstance("copySample");
        this.dialogContainer = document.getElementById("mapIndexSyntaxDialog");
    }

    copySample(sampleTitle: string) {
        const sampleText = mapIndexSyntax.samples.find(x => x.title === sampleTitle).text;
        copyToClipboard.copy(sampleText, "Sample has been copied to clipboard", this.dialogContainer);
    }

    static readonly samples: Array<sampleCode> = [
        {
            title: "Linq ",
            text:
`from company in docs.Companies
where company.Address.Country == "USA"
select new {
     company.Name,
     company.Phone,
     City = company.Address.City
}`,
            html:
`<span class="token keyword">from </span>company <span class="token keyword">in </span><span class="token builtin">docs.Companies</span>
<span class="token keyword">where </span>company.Address.Country <span class="token operator">== </span>"USA"
<span class="token keyword">select new </span>{
     company.<span class="token string">Name</span>,
     company.<span class="token string">Phone</span>,
     City = company.Address.<span class="token string">City</span>
}


`
        },
        {
            title: "JavaScript ",
            text:
`map("Companies", (company) => {
    if (company.Address.Country === "USA") {
        return {
            Name: company.Name,
            Phone: company.Phone,
            City: company.Address.City
        };
    }
})`,
            html:
`<span class="token keyword">map</span>(<span class="token string">"Companies"</span>, (company) <span class="token operator">=></span> {
    <span class="token keyword">if </span>(company.Address.Country <span class="token operator">=== </span>"USA") {
        <span class="token keyword">return</span> {
            Name: company.<span class="token string">Name</span>,
            Phone: company.<span class="token string">Phone</span>,
            City: company.Address.<span class="token string">City</span>
        };
    }
})`
        }
    ];
}

export = mapIndexSyntax;
