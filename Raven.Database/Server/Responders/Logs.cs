using System.Collections.Generic;
using Raven.Abstractions.Logging;
using Raven.Database.Server.Abstractions;
using System.Linq;
using Raven.Database.Util;
using Raven.Database.Extensions;

namespace Raven.Database.Server.Responders
{
	public class Logs : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/logs$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new string[]{"GET"};}
		}

		public override void Respond(IHttpContext context)
		{
			var boundedMemoryTarget = LogManager.GetTarget<BoundedMemoryTarget>();
			if(boundedMemoryTarget == null)
			{
				context.SetStatusToNotFound();
				context.WriteJson(new
				{
					Error = "HttpEndpoint was not registered in the log configuration, logs endpoint disabled"
				});
				return;
			}
			IEnumerable<LogEventInfo> log = boundedMemoryTarget.GeneralLog;

			switch (context.Request.QueryString["type"])
			{
				case "error":
				case "warn":
					log = boundedMemoryTarget.WarnLog;
					break;
			}

			context.WriteJson(log.Select(x => new
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