namespace Raven.Studio.Messages
{
	using Database.Data;

	public class StatisticsUpdated
	{
		public StatisticsUpdated(DatabaseStatistics statistics) { Statistics = statistics; }

		public DatabaseStatistics Statistics { get; private set; }
		public bool HasDocumentCountChanged { get; set; }
	}
}