namespace Raven.ManagementStudio.UI.Silverlight.Models
{
    using Client.Silverlight.Document;
    using Plugin;

    public class Database : IDatabase
    {
        public Database(string databaseAdress, string databaseName = null)
        {
            this.Address = databaseAdress;
            this.Name = databaseName ?? databaseAdress;
            var store = new DocumentStore(databaseAdress);
            this.Session = store.OpenAsyncSession();
        }

        public string Address { get; set; }

        public string Name { get; set; }

        public IAsyncDocumentSession Session { get; set; }
    }
}