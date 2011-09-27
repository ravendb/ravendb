using System;
using Raven.Database.Server.Abstractions;
using System.Linq;
using Raven.Database.Util;
using Raven.Database.Extensions;

namespace Raven.Database.Server.Responders
{
	public class Logs : RequestResponder
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
			var boundedMemoryTarget = NLog.LogManager.Configuration.AllTargets.OfType<BoundedMemoryTarget>().FirstOrDefault();
			if(boundedMemoryTarget == null)
			{
				context.SetStatusToNotFound();
				context.WriteJson(new
				{
					Error = "HttpEndpoint was not registered in the log configuration, logs endpoint disable"
				});
				return;
			}

			context.WriteJson(boundedMemoryTarget.GetSnapshot().Select(x => new
			{
				x.TimeStamp,
				Message = x.FormattedMessage,
				x.LoggerName,
				x.Level.Name,
				Exception = x.Exception == null ? null : x.Exception.ToString()
			}));
		}
	}
}