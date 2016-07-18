namespace Raven.Client.Replication.Messages
{
    public class ReplicationLatestEtag
    {
		public string SourceDatabaseName { get; set; }

		public string SourceDatabaseId { get; set; }

		public string SourceUrl { get; set; }

		public string SourceMachineName { get; set; }
	}
}
