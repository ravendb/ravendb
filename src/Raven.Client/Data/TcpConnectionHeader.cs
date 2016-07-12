using System;

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

		public string SourceDatabaseName { get; set; }

		public string SourceDatabaseId { get; set; }

		public string SourceUrl { get; set; }

		public OperationTypes Operation { get; set; }
	}
}