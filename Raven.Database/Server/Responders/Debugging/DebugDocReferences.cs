// -----------------------------------------------------------------------
//  <copyright file="DebugDocReferences.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;

namespace Raven.Database.Server.Responders.Debugging
{
	public class DebugDocReferences : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/debug/docrefs/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var docId = Uri.UnescapeDataString(match.Groups[1].Value);

			int totalCount = -1;
			List<string> results = null;
			Database.TransactionalStorage.Batch(accessor =>
			{
				totalCount = accessor.Indexing.GetCountOfDocumentsReferencing(docId);
				results =
					accessor.Indexing.GetDocumentsReferencing(docId)
					        .Skip(context.GetStart())
					        .Take(context.GetPageSize(Database.Configuration.MaxPageSize))
					        .ToList();
			});

			context.WriteJson(new
			{
				TotalCount = totalCount,
				Results = results
			});
		}
	}
}