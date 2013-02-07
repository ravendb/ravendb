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
					var totalIndexSizeOnDisk = Database.GetIndexStorageSizeOnDisk();
					var totalDatabaseSizeOnDisk = Database.GetTransactionalStorageSizeOnDisk();
					var totalSizeOnDisk = totalIndexSizeOnDisk + totalDatabaseSizeOnDisk;
					context.WriteJson(new
					{
						TransactionalStorageSize = totalDatabaseSizeOnDisk,
						TransactionalStorageSizeHumane = DatabaseSize.Humane( totalDatabaseSizeOnDisk ),
						IndexStorageSize = totalIndexSizeOnDisk,
						IndexStorageSizeHumane = DatabaseSize.Humane( totalIndexSizeOnDisk ),
						TotalDatabaseSize = totalSizeOnDisk,
						TotalDatabaseSizeHumane = DatabaseSize.Humane( totalSizeOnDisk ),
					} );
					break;
			}
		}
	}
}