import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import copyToClipboard = require("common/copyToClipboard");

class additionalAssemblySyntax extends dialogViewModelBase {

    htmlElement: HTMLElement;

    compositionComplete() {
        super.compositionComplete();
        this.bindToCurrentInstance("copySample");
        this.htmlElement = document.getElementById("additionalAssymblySyntaxDialog");
    }

    copySample(sampleTitle: string) {
        const sampleText = additionalAssemblySyntax.csharpSamples.find(x => x.title === sampleTitle).text;
        copyToClipboard.copy(sampleText, "Sample has been copied to clipboard", this.htmlElement);
    }

    static readonly additionalSourceCsharpText =
`// *Additional Source File*
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Wordprocessing;

public static class Office
{
    public static IEnumerable<string> GetWordText(Stream stream)
    {   
        // Use methods from NuGet package DocumentFormat.OpenXml
        using var doc = WordprocessingDocument.Open(stream, false);
        foreach (var element in doc.MainDocumentPart.Document.Body)
        {
            if (element is Paragraph p) {
                yield return p.InnerText;
            }
        }
    }
}
`;

    static readonly usageInMapCsharpText =
`// *Usage in the index Map statement* 
from doc in docs.FilesCollection
let attachments = AttachmentsFor(doc).Where(x => x.Name.EndsWith(".docx"))
                                     .Select(x => LoadAttachment(doc, x.Name).GetContentAsStream())
select new {
    Documents = new[] {
        // Use method from source file
        attachments.Select(attachment => Office.GetWordText(attachment))
    }
}
`;

    static readonly csharpSamples: Array<sampleCode> = [
        {
            title: "Csharp - Additional Source",
            text: additionalAssemblySyntax.additionalSourceCsharpText,
            html: Prism.highlight(additionalAssemblySyntax.additionalSourceCsharpText, (Prism.languages as any).csharp)
        },
        {
            title: "Csharp - Usage in Map",
            text: additionalAssemblySyntax.usageInMapCsharpText,
            html: Prism.highlight(additionalAssemblySyntax.usageInMapCsharpText, (Prism.languages as any).csharp)
        }
    ];
}

export = additionalAssemblySyntax;
