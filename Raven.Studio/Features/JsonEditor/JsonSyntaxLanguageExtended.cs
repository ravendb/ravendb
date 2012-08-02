using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using ActiproSoftware.Text.Tagging;
using ActiproSoftware.Text.Tagging.Implementation;
using ActiproSoftware.Windows.Controls.SyntaxEditor;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Adornments.Implementation;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt.Implementation;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining.Implementation;
using Raven.Studio.Controls;
using ActiproSoftware.Text;

namespace Raven.Studio.Features.JsonEditor
{
    public class JsonSyntaxLanguageExtended : JsonSyntaxLanguage
    {
        public JsonSyntaxLanguageExtended() : base()
        { 
            this.RegisterService<IOutliner>(new JsonOutliner());

            this.RegisterParser(new JsonParser());

            RegisterService<ITextFormatter>(new JsonTextFormatter());

            this.RegisterService(new TextViewTaggerProvider<WordHighlightTagger>(typeof(WordHighlightTagger)));

            // Register a tagger provider for showing parse errors
            this.RegisterService<ICodeDocumentTaggerProvider>(new CodeDocumentTaggerProvider<ParseErrorTagger>(typeof(ParseErrorTagger)));

            // Register a squiggle tag quick info provider
            this.RegisterService<IQuickInfoProvider>(new SquiggleTagQuickInfoProvider());


            this.RegisterService(new AdornmentManagerProvider<LinkTagAdornmentManager>(typeof(LinkTagAdornmentManager)));

            this.RegisterService(new CodeDocumentTaggerProvider<PropertyValueLinkTagger>(typeof(PropertyValueLinkTagger)));

            this.RegisterService<IEditorViewMouseInputEventSink>(new LinkTagClickHandler());

            this.RegisterService<IQuickInfoProvider>(new LinkTagQuickInfoProvider());
        }
    }
}
