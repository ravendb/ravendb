namespace Raven.Studio.Features.Documents
{
    public class ColumnDefinition
    {
        public string Header { get; set; }

        /// <summary>
        /// The binding is a property path relative to a JsonDocument, e.g. DataAsJson[Title]
        /// </summary>
        public string Binding { get; set; }

        public string DefaultWidth { get; set; }
    }
}