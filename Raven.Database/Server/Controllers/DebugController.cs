using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using Raven.Database.Linq;
using Raven.Database.Linq.Ast;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public class DebugController : RavenApiController
	{
		[HttpGet]
		public HttpResponseMessage Changes()
		{
			return GetMessageWithObject(Database.TransportState.DebugStatuses);
		}

		[HttpGet]
		public HttpResponseMessage Config()
		{
			var cfg = RavenJObject.FromObject(Database.Configuration);
			cfg["OAuthTokenKey"] = "<not shown>";
			var changesAllowed = Database.Configuration.Settings["Raven/Versioning/ChangesToRevisionsAllowed"];
			if (string.IsNullOrWhiteSpace(changesAllowed) == false)
				cfg["Raven/Versioning/ChangesToRevisionsAllowed"] = changesAllowed;

			return GetMessageWithObject(cfg);
		}

		[HttpGet]
		public HttpResponseMessage Docrefs(string id)
		{
			var totalCount = -1;
			List<string> results = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				totalCount = accessor.Indexing.GetCountOfDocumentsReferencing(id);
				results =
					accessor.Indexing.GetDocumentsReferencing(id)
							.Skip(GetStart())
							.Take(GetPageSize(Database.Configuration.MaxPageSize))
							.ToList();
			});

			return GetMessageWithObject(new
			{
				TotalCount = totalCount,
				Results = results
			});
		}

		[HttpGet("debug/index-fields")]
		public async Task<HttpResponseMessage> IndexFields()
		{
			var indexStr = await ReadStringAsync();
			var mapDefinition = indexStr.Trim().StartsWith("from")
				? QueryParsingUtils.GetVariableDeclarationForLinqQuery(indexStr, true)
				: QueryParsingUtils.GetVariableDeclarationForLinqMethods(indexStr, true);

			var captureSelectNewFieldNamesVisitor = new CaptureSelectNewFieldNamesVisitor();
			mapDefinition.AcceptVisitor(captureSelectNewFieldNamesVisitor, null);

			return GetMessageWithObject(new { captureSelectNewFieldNamesVisitor.FieldNames });
		}

		[HttpGet]
		public HttpResponseMessage List(string id)
		{
			var listName = id;
			var key = Request.RequestUri.ParseQueryString()["key"];
			if (key == null)
				throw new ArgumentException("Key query string variable is mandatory");

			ListItem listItem = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				listItem = accessor.Lists.Read(listName, key);
				if (listItem == null)
					throw new HttpException(400, "Not found");

			});

			if (listItem == null)
				throw new HttpException(400, "Not found");

			return GetMessageWithObject(listItem);
		}

		[HttpGet]
		public HttpResponseMessage Queries()
		{
			return GetMessageWithObject(Database.WorkContext.CurrentlyRunningQueries);
		}
	}
}