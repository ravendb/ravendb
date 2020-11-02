import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class additionalSourceSyntax extends dialogViewModelBase {

    htmlElement: HTMLElement;

    compositionComplete() {
        super.compositionComplete();
        this.bindToCurrentInstance("copySample");
        this.htmlElement = document.getElementById("additionalSourceSyntaxDialog");
    }

    copySample(sampleTitle: string) {
        let sampleArray = sampleTitle.startsWith("Csharp") ? additionalSourceSyntax.csharpSamples : additionalSourceSyntax.javascriptSamples;
        const sampleText = sampleArray.find(x => x.title === sampleTitle).text;
        copyToClipboard.copy(sampleText, "Sample has been copied to clipboard", this.htmlElement);
    }

    static readonly additionalSourceCsharpText =
`// *Additional Source*
public static class PeopleUtil
{
    public static string CalculatePersonEmail(string name)
    {
        return $"{name}@example.com";
    }
}`;

    static readonly usageInMapCsharpText =
`// *Usage in Map statement* 
docs.People.Select(person => new {
    Name = person.Name,
    _ = this.CreateField("Email", 
            PeopleUtil.CalculatePersonEmail(person.Name), 
            stored: true)
})


`;

    static readonly additionalSourceJavascriptText =
`// *Additional Source*


function calculatePersonEmail(name) 
{
    return name + '@example.com';
}

`;

    static readonly usageInMapJavascriptText =
`// *Usage in Map statement* 
map('People', function (person) { return {
       Name: person.Name, 
       _: { $name:'Email',
            $value: calculatePersonEmail(person.Name),
            $options:{ store: true }
        }
    };
})`;

    static readonly csharpSamples: Array<sampleCode> = [
        {
            title: "Csharp - Additional Source",
            text: additionalSourceSyntax.additionalSourceCsharpText,
            html: ko.pureComputed(() => {
                return Prism.highlight(additionalSourceSyntax.additionalSourceCsharpText, (Prism.languages as any).csharp);
            })
        },
        {
            title: "Csharp - Usage in Map",
            text: additionalSourceSyntax.usageInMapCsharpText,
            html: ko.pureComputed(() => {
                return Prism.highlight(additionalSourceSyntax.usageInMapCsharpText, (Prism.languages as any).csharp);
            })
        }
    ];
    
    static readonly javascriptSamples: Array<sampleCode> = [
        {
            title: "Javascript - Additional Source",
            text: additionalSourceSyntax.additionalSourceJavascriptText,
            html: ko.pureComputed(() => {
                return Prism.highlight(additionalSourceSyntax.additionalSourceJavascriptText, (Prism.languages as any).javascript);
            })
        },
        {
            title: "Javascript - Usage in Map",
            text: additionalSourceSyntax.usageInMapJavascriptText,
            html: ko.pureComputed(() => {
                return Prism.highlight(additionalSourceSyntax.usageInMapJavascriptText, (Prism.languages as any).javascript);
            })
        }
    ];
}

export = additionalSourceSyntax;
