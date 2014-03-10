using System;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
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
	}
}