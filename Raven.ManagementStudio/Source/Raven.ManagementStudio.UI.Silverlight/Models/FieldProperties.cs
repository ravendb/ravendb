using Raven.Database.Indexing;

namespace Raven.ManagementStudio.UI.Silverlight.Models
{
    public class FieldProperties
    {
        public string Name { get; set; }

        public FieldStorage Storage { get; set; }

        public FieldIndexing Indexing { get; set; }

        public SortOptions Sort { get; set; }

        public string Analyzer { get; set; }
    }
}
