using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Util;

namespace Raven.Database.Server.Controllers
{
	public class LogsController : RavenApiController
	{
		[HttpGet][Route("logs")]
		public HttpResponseMessage LogsGet()
		{
			var target = LogManager.GetTarget<DatabaseMemoryTarget>();
			if (target == null)
			{
				return GetMessageWithObject(new
				{
					Error = "DatabaseMemoryTarget was not registered in the log manager, logs endpoint disabled"
				}, HttpStatusCode.NotFound);
			}

			var database = Database;
			if (database == null)
				return GetMessageWithObject(new {Error = "No database found."}, HttpStatusCode.BadRequest);
			
			var dbName = database.Name ?? Constants.SystemDatabase;
			var boundedMemoryTarget = target[dbName];
			var log = boundedMemoryTarget.GeneralLog;

			switch (GetQueryStringValue("type"))
			{
				case "error":
				case "warn":
					log = boundedMemoryTarget.WarnLog;
					break;
			}

			return GetMessageWithObject(log.Select(x => new
			{
				x.TimeStamp,
				Message = x.FormattedMessage,
				x.LoggerName,
				Level = x.Level.ToString(),
				Exception = x.Exception == null ? null : x.Exception.ToString()
			}));
		}
	}
}