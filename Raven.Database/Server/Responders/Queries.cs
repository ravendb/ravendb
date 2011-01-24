//-----------------------------------------------------------------------
// <copyright file="Queries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

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
			JArray itemsToLoad;
			if(context.Request.HttpMethod == "POST")
				itemsToLoad = context.ReadJsonArray();
			else
				itemsToLoad = new JArray(context.Request.QueryString.GetValues("id"));
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
				foreach (JToken item in itemsToLoad)
				{
					var value = item.Value<string>();
					if(loadedIds.Add(value)==false)
						continue;
					var documentByKey = Database.Get(value, transactionInformation);
					if (documentByKey == null)
						continue;
					result.Results.Add(documentByKey.ToJson());

					includedEtags.AddRange(documentByKey.Etag.ToByteArray());
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

			context.Response.AddHeader("ETag", computedEtag.ToString());
			context.WriteJson(result);
		}
	}
}
