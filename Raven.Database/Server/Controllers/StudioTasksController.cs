using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Database.Smuggler;

namespace Raven.Database.Server.Controllers
{
	public class StudioTasksController : RavenDbApiController
	{
		[HttpPost]
		[Route("studio-tasks/import")]
		[Route("databases/{databaseName}/studio-tasks/import")]
		public async Task<HttpResponseMessage> ImportDatabase()
		{
			var dataDumper = new DataDumper(Database);
			var importData = dataDumper.ImportData(new SmugglerImportOptions
			{
				FromStream = await InnerRequest.Content.ReadAsStreamAsync()
			}, new SmugglerOptions());
			throw new InvalidOperationException();
		}

		[HttpPost]
		[Route("studio-tasks/createSampleData")]
		[Route("databases/{databaseName}/studio-tasks/createSampleData")]
		public async Task<HttpResponseMessage> CreateSampleData()
		{
			var results = Database.Queries.Query(Constants.DocumentsByEntityNameIndex, new IndexQuery(), CancellationToken.None);
			if (results.Results.Count > 0)
			{
				return GetMessageWithString("You cannot create sample data in a database that already contains documents", HttpStatusCode.BadRequest);
			}

			using (var sampleData = typeof(StudioTasksController).Assembly.GetManifestResourceStream("Raven.Database.Server.Assets.EmbeddedData.Northwind.dump"))
			{
				var smugglerOptions = new SmugglerOptions
				{
					OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Transformers,
					ShouldExcludeExpired = false,
				};
				var dataDumper = new DataDumper(Database);
				await dataDumper.ImportData(new SmugglerImportOptions {FromStream = sampleData}, smugglerOptions);
			}

			return GetEmptyMessage();
		}
	}
}