namespace Raven.ManagementStudio.Plugin
{
	using Client;

    public interface IDatabase
    {
        string Address { get; set; }

        string Name { get; set; }

        IAsyncDocumentSession Session { get; set; }

		//IAsyncAttachmentSession AttachmentSession { get; set; }

		//IAsyncCollectionSession CollectionSession { get; set; }

		//IAsyncIndexSession IndexSession { get; set; }

		//IAsyncStatisticsSession StatisticsSession { get; set; }
    }
}