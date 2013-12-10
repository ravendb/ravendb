using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using NLog;
using Raven.Database.Util;

namespace Raven.Database.Server.RavenFS.Controllers
{
	//TODO: check this class
	public class LogsController : ApiController
	{
		public HttpResponseMessage Get(string type = null)
		{
			DatabaseMemoryTarget.BoundedMemoryTarget boundedMemoryTarget = null;
			if (LogManager.Configuration != null && LogManager.Configuration.AllTargets != null)
				boundedMemoryTarget = LogManager.Configuration.AllTargets.OfType<DatabaseMemoryTarget.BoundedMemoryTarget>().FirstOrDefault();

			if (boundedMemoryTarget == null)
				throw new HttpResponseException(Request.CreateErrorResponse(HttpStatusCode.PreconditionFailed,
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
	}
}