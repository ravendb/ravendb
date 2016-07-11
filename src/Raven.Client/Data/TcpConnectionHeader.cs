namespace Raven.Abstractions.Data
{
	public class TcpConnectionHeaderMessage
	{
		public enum OperationTypes
		{
			None,
			BulkInsert,
			Subscription,
			Replication
		}

		public string DatabaseName { get; set; }

		public string DatabaseId { get; set; }

		public OperationTypes Operation { get; set; }
	}
}