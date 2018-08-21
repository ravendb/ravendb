import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class additionalSourceSyntax extends dialogViewModelBase {


    static readonly additionalSourceCsharp = `// *Additional Source*
public static class PeopleUtil
{
    public static string CalculatePersonEmail(string name)
    {
        return $"{name}@example.com";
    }
}`;
    
    static readonly usageInMapCsharp = `// *Usage in Map statement* 
docs.People.Select(person => new {
    Name = person.Name,
    _ = this.CreateField("Email", 
               PeopleUtil.CalculatePersonEmail(person.Name), 
               stored: true)
})


`;

    static readonly additionalSourceJavascript = `// *Additional Source*


function calculatePersonEmail(name) 
{
    return name + '@example.com';
}

`;

    static readonly usageInMapJavascript = `// *Usage in Map statement* 
map('People', function (person) { return {
       Name: person.Name, 
       _: { $name:'Email',
            $value: calculatePersonEmail(person.Name),
            $options:{ store: true }
        }
    };
})`;

    additionalSourceCsharpHtml = ko.pureComputed(() => {
        return Prism.highlight(additionalSourceSyntax.additionalSourceCsharp, (Prism.languages as any).csharp);
    });
    usageInMapCsharpHtml = ko.pureComputed(() => {
        return Prism.highlight(additionalSourceSyntax.usageInMapCsharp, (Prism.languages as any).csharp);
    });
    additionalSourceJavascriptHtml = ko.pureComputed(() => {
        return Prism.highlight(additionalSourceSyntax.additionalSourceJavascript, (Prism.languages as any).javascript);
    });
    usageInMapJavascriptHtml = ko.pureComputed(() => {
        return Prism.highlight(additionalSourceSyntax.usageInMapJavascript, (Prism.languages as any).javascript);
    });
    
}
export = additionalSourceSyntax;
