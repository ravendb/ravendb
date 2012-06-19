//-----------------------------------------------------------------------
// <copyright file="Queries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders
{
	public class Queries : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/queries/?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"POST","GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			RavenJArray itemsToLoad;
			if(context.Request.HttpMethod == "POST")
				itemsToLoad = context.ReadJsonArray();
			else
				itemsToLoad = new RavenJArray(context.Request.QueryString.GetValues("id"));
			var result = new MultiLoadResult();
			var loadedIds = new HashSet<string>();
			var includes = context.Request.QueryString.GetValues("include") ?? new string[0];
			var transactionInformation = GetRequestTransaction(context);
		    var includedEtags = new List<byte>();
			Database.TransactionalStorage.Batch(actions =>
			{
				var addIncludesCommand = new AddIncludesCommand(Database, transactionInformation, (etag, includedDoc) =>
				{
					includedEtags.AddRange(etag.ToByteArray());
				    result.Includes.Add(includedDoc);
				}, includes, loadedIds);
				foreach (RavenJToken item in itemsToLoad)
				{
					var value = item.Value<string>();
					if(loadedIds.Add(value)==false)
						continue;
					var documentByKey = Database.Get(value, transactionInformation);
					if (documentByKey == null)
						continue;
					result.Results.Add(documentByKey.ToJson());

					if (documentByKey.Etag != null)
					{
						includedEtags.AddRange(documentByKey.Etag.Value.ToByteArray());
					}
					includedEtags.Add((documentByKey.NonAuthoritativeInformation ?? false) ? (byte)0 : (byte)1);
					addIncludesCommand.Execute(documentByKey.DataAsJson);
				}
			});

			Guid computedEtag;

			using (var md5 = MD5.Create())
			{
				var computeHash = md5.ComputeHash(includedEtags.ToArray());
				computedEtag = new Guid(computeHash);
			}

			if (context.MatchEtag(computedEtag))
			{
				context.SetStatusToNotModified();
				return;
			}

			context.WriteETag(computedEtag);
			context.WriteJson(result);
		}
	}
}
