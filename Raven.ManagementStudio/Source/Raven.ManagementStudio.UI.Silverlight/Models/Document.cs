namespace Raven.ManagementStudio.UI.Silverlight.Models
{
    using Client.Silverlight.Data;

    public class Document
    {
        private JsonDocument document;

        public Document(JsonDocument jsonDocument)
        {
            this.document = jsonDocument;
            this.Id = jsonDocument.Id;
        }

        public string Id { get; set; }
    }
}
