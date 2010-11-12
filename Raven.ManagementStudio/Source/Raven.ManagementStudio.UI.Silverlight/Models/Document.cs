namespace Raven.ManagementStudio.UI.Silverlight.Models
{
    using Client.Silverlight.Data;

    public class Document
    {
        public Document(JsonDocument jsonDocument)
        {
            this.Id = jsonDocument.Id;
        }

        public string Id { get; set; }
    }
}
