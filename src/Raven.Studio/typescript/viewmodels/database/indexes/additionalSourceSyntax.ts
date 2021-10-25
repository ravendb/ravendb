import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");
import { highlight, languages } from "prismjs";

class additionalSourceSyntax extends dialogViewModelBase {

    view = require("views/database/indexes/additionalSourceSyntax.html");
    additionalTextView = require("views/database/indexes/additionalTabsCommonText.html");
    
    dialogContainer: Element;

    compositionComplete() {
        super.compositionComplete();
        this.bindToCurrentInstance("copySample");
        this.dialogContainer = document.getElementById("additionalSourceSyntaxDialog");
    }

    copySample(sampleTitle: string) {
        const sampleText = [...additionalSourceSyntax.csharpSamples, ...additionalSourceSyntax.javascriptSamples]
            .find(x => x.title === sampleTitle).text;

        copyToClipboard.copy(sampleText, "Sample has been copied to clipboard", this.dialogContainer);
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
`// *Usage in the index Map statement* 
docs.People.Select(person => new {
    Name = person.Name,
    _ = this.CreateField("Email", 
            // use method from source file
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
`// *Usage in the index Map statement* 
map('People', function (person) { return {
       Name: person.Name, 
       _: { $name:'Email',
            // use method from source file
            $value: calculatePersonEmail(person.Name),
            $options:{ store: true }
        }
    };
})`;

    static readonly csharpSamples: Array<sampleCode> = [
        {
            title: "Csharp - Additional Source",
            text: additionalSourceSyntax.additionalSourceCsharpText,
            html: highlight(additionalSourceSyntax.additionalSourceCsharpText, languages.csharp, "csharp")
        },
        {
            title: "Csharp - Usage in Map",
            text: additionalSourceSyntax.usageInMapCsharpText,
            html: highlight(additionalSourceSyntax.usageInMapCsharpText, languages.csharp, "csharp")
        }
    ];

    static readonly javascriptSamples: Array<sampleCode> = [
        {
            title: "Javascript - Additional Source",
            text: additionalSourceSyntax.additionalSourceJavascriptText,
            html: highlight(additionalSourceSyntax.additionalSourceJavascriptText, languages.javascript, "csharp")
        },
        {
            title: "Javascript - Usage in Map",
            text: additionalSourceSyntax.usageInMapJavascriptText,
            html: highlight(additionalSourceSyntax.usageInMapJavascriptText, languages.javascript, "csharp")
        }
    ];
}

export = additionalSourceSyntax;
