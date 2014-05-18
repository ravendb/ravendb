using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Util;

namespace Raven.Database.Server.Controllers
{
    [Route("databases/{databaseName}/logs/{action=logsget}")]
	[Route("logs/{action=logsget}")]
	[Route("logs/ravenfs/{action=RavenFSLogsGet}")]
	public class LogsController : RavenDbApiController
	{
		[HttpGet]		
		public HttpResponseMessage RavenFSLogsGet(string type = null)
		{
			DatabaseMemoryTarget.BoundedMemoryTarget boundedMemoryTarget = null;
			if (NLog.LogManager.Configuration != null && NLog.LogManager.Configuration.AllTargets != null)
				boundedMemoryTarget = NLog.LogManager.Configuration.AllTargets.OfType<DatabaseMemoryTarget.BoundedMemoryTarget>().FirstOrDefault();

			if (boundedMemoryTarget == null)
				throw new HttpResponseException(Request.CreateErrorResponse((HttpStatusCode)420,
																			"HttpEndpoint was not registered in the log configuration, logs endpoint disable"));

			var log = boundedMemoryTarget.GeneralLog;

			switch (type)
			{
				case "error":
				case "warn":
					log = boundedMemoryTarget.WarnLog;
					break;
			}

			return Request.CreateResponse(HttpStatusCode.OK, log.Select(x => new
			{
				x.TimeStamp,
				Message = x.FormattedMessage,
				x.LoggerName,
				Level = x.Level,
				Exception =
			x.Exception == null ? null : x.Exception.ToString()
			}));
		}


		[HttpGet]
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