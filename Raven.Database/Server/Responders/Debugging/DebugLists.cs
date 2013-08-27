// -----------------------------------------------------------------------
//  <copyright file="DebugLists.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders.Debugging
{
	public class DebugLists : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/debug/lists/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var listName = Uri.UnescapeDataString(match.Groups[1].Value);
			var key = context.Request.QueryString["key"];
			if (key == null)
				throw new ArgumentException("Key query string variable is mandatory");
			Database.TransactionalStorage.BatchRead(accessor =>
			{
				var listItem = accessor.Lists.Read(listName, key);
				if (listItem == null)
				{
					context.SetStatusToNotFound();
					context.WriteJson(new {Error = "Not Found"});
				}
				else
				{
					context.WriteJson(listItem);
				}
			});
			
		}
	}
}