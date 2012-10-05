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
			var target = LogManager.GetTarget<DatabaseMemoryTarget>();
			if(target == null)
			{
				context.SetStatusToNotFound();
				context.WriteJson(new
				{
					Error = "DatabaseMemoryTarget was not registered in the log manager, logs endpoint disabled"
				});
				return;
			}
			var database = Database;
			if(database == null)
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "No database found."
				});
				return;
			}
			if(database.Name == null)
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "Database name is null."
				});
				return;
			}
			IEnumerable<LogEventInfo> log = target[Database.Name].GeneralLog;

			switch (context.Request.QueryString["type"])
			{
				case "error":
				case "warn":
					log = target[Database.Name].WarnLog;
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