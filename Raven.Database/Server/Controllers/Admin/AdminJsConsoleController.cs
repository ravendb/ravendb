// -----------------------------------------------------------------------
//  <copyright file="AdminJsConsoleController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Database.JsConsole;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Server.Controllers.Admin
{
	[RoutePrefix("")]
	public class AdminJsConsoleController : BaseAdminController
	{
		[HttpPost]
		[RavenRoute("admin/console/{*id}")]
		public async Task<HttpResponseMessage> Console(string id)
		{
			var database = await DatabasesLandlord.GetDatabaseInternal(id);

			var script = await ReadJsonObjectAsync<AdminJsScript>();

			var console = new AdminJsConsole(database);

			var result = console.ApplyScript(script);

			return GetMessageWithObject(result);
		}
	}
}