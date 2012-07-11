using System;
using System.Collections.Generic;
using NLog;
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
			BoundedMemoryTarget boundedMemoryTarget = null;
			if (LogManager.Configuration != null && LogManager.Configuration.AllTargets != null)
			{
				boundedMemoryTarget = LogManager.Configuration.AllTargets.OfType<BoundedMemoryTarget>().FirstOrDefault();
			}
			if(boundedMemoryTarget == null)
			{
				context.SetStatusToNotFound();
				context.WriteJson(new
				{
					Error = "HttpEndpoint was not registered in the log configuration, logs endpoint disable"
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
				Level = x.Level.Name,
				Exception = x.Exception == null ? null : x.Exception.ToString()
			}));
		}
	}
}