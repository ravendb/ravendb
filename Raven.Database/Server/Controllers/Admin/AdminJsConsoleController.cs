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
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers.Admin
{
	[RoutePrefix("")]
	public class AdminJsConsoleController : BaseAdminDatabaseApiController
	{
		[HttpPost]
		[RavenRoute("admin/console/{*id}")]
		public async Task<HttpResponseMessage> Console(string id)
		{
			var database = await DatabasesLandlord.GetResourceInternal(id).ConfigureAwait(false);

			var script = await ReadJsonObjectAsync<AdminJsScript>().ConfigureAwait(false);

			var console = new AdminJsConsole(database);

			var result = console.ApplyScript(script);

			if (result.Type == JTokenType.Null)
			{
				result = new RavenJValue("OK");
			}
			return GetMessageWithObject(result);
		}
	}
}