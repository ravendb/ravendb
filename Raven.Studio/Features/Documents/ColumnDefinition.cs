using Raven.Imports.Newtonsoft.Json;

namespace Raven.Studio.Features.Documents
{
    public class ColumnDefinition
    {
        public string Header { get; set; }


        public string Binding { get; set; }

        public string DefaultWidth { get; set; }
    }
}