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
using ActiproSoftware.Text;
using ActiproSoftware.Text.Parsing.LLParser;
using ActiproSoftware.Text.Parsing.LLParser.Implementation;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining;

namespace Raven.Studio.Features.JsonEditor
{
    public class JsonOutliner : IOutliner
    {
        public IOutliningSource GetOutliningSource(ITextSnapshot snapshot)
        {
            if (snapshot != null)
            {
                // Create an outlining source based on the parse data
                var source = new JsonOutliningSource(snapshot);

                // Translate the data to the desired snapshot, which could be slightly newer than the parsed source
                source.TranslateTo(snapshot);

                return source;
            }
            return null;
        }

        public AutomaticOutliningUpdateTrigger UpdateTrigger
        {
            get
            {
                return AutomaticOutliningUpdateTrigger.ParseDataChanged;
            }
        }
    }
}
