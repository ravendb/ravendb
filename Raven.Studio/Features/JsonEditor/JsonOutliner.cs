using ActiproSoftware.Text;
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