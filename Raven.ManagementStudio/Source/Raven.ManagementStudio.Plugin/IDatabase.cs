namespace Raven.ManagementStudio.Plugin
{
    using Client.Silverlight.Document;

    public interface IDatabase
    {
        string Address { get; set; }

        string Name { get; set; }

        IAsyncDocumentSession Session { get; set; }
    }
}