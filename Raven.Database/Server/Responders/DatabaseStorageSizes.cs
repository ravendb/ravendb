using System;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class DatabaseStorageSizes : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/database/storage/sizes?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			switch (context.Request.HttpMethod)
			{
				case "GET":
					var indexStorageSize = Database.GetIndexStorageSizeOnDisk();
					var transactionalStorageSize = Database.GetTransactionalStorageSizeOnDisk();
					var totalDatabaseSize = indexStorageSize + transactionalStorageSize;
					context.WriteJson(new
					{
						TransactionalStorageSize = transactionalStorageSize,
						TransactionalStorageSizeHumane = DatabaseSize.Humane( transactionalStorageSize ),
						IndexStorageSize = indexStorageSize,
						IndexStorageSizeHumane = DatabaseSize.Humane( indexStorageSize ),
						TotalDatabaseSize = totalDatabaseSize,
						TotalDatabaseSizeHumane = DatabaseSize.Humane( totalDatabaseSize ),
					} );
					break;
			}
		}
	}
}