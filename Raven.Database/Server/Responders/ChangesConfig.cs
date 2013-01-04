// -----------------------------------------------------------------------
//  <copyright file="ChangesConfig.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;

namespace Raven.Database.Server.Responders
{
	public class ChangesConfig : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/changes/config$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var id = context.Request.QueryString["id"];
			if (string.IsNullOrEmpty(id))
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
									{
										Error = "id query string parameter is mandatory when using changes/config endpoint"
									});
				return;
			}

			var name = context.Request.QueryString["value"];
			var connectionState = Database.TransportState.For(id);
			var cmd = context.Request.QueryString["command"];
			if (Match(cmd, "disconnect"))
			{
				Database.TransportState.Disconnect(id);
			}
			else if (Match(cmd, "watch-index"))
			{
				connectionState.WatchIndex(name);
			}
			else if (Match(cmd, "unwatch-index"))
			{
				connectionState.UnwatchIndex(name);
			}
			else if (Match(cmd, "watch-indexes"))
			{
				connectionState.WatchAllIndexes();
			}
			else if (Match(cmd, "unwatch-indexes"))
			{
				connectionState.UnwatchAllIndexes();
			}
			else if (Match(cmd, "watch-doc"))
			{
				connectionState.WatchDocument(name);
			}
			else if (Match(cmd, "unwatch-doc"))
			{
				connectionState.UnwatchDocument(name);
			}
			else if (Match(cmd, "watch-docs"))
			{
				connectionState.WatchAllDocuments();
			}
			else if (Match(cmd, "unwatch-docs"))
			{
				connectionState.UnwatchAllDocuments();
			}
			else if (Match(cmd, "watch-prefix"))
			{
				connectionState.WatchDocumentPrefix(name);
			}
			else if (Equals(cmd, "unwatch-prefix"))
			{
				connectionState.UnwatchDocumentPrefix(name);
			}
			else
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "command argument is mandatory"
				});
			}
		}

		private bool Match(string x, string y)
		{
			return string.Equals(x, y, StringComparison.InvariantCultureIgnoreCase);
		}
	}
}