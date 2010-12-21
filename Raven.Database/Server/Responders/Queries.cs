//-----------------------------------------------------------------------
// <copyright file="Queries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
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
            var computedEtagBytes = new byte[16];
		    var includedEtags = new List<Guid>();
            Database.TransactionalStorage.Batch(actions =>
			{
				var addIncludesCommand = new AddIncludesCommand(Database, transactionInformation, (etag, includedDoc) =>
				{
                    includedEtags.Add(etag);
				    result.Includes.Add(includedDoc);
				}, includes, loadedIds);
				foreach (JToken item in itemsToLoad)
				{
					var value = item.Value<string>();
					if(loadedIds.Add(value)==false)
						continue;
					var documentByKey = actions.Documents.DocumentByKey(value,
                        transactionInformation);
					if (documentByKey == null)
						continue;
					result.Results.Add(documentByKey.ToJson());

				    var etagBytes = documentByKey.Etag.ToByteArray();
				    for (int i = 0; i < 16; i++)
				    {
				        computedEtagBytes[i] ^= etagBytes[i];
				    }

				    addIncludesCommand.Execute(documentByKey.DataAsJson);
				}
            });

		    foreach (var includedGuid in includedEtags)
		    {
                var etagBytes = includedGuid.ToByteArray();
                for (int i = 0; i < 16; i++)
                {
                    computedEtagBytes[i] ^= etagBytes[i];
                }
		    }
		    var computedEtag = new Guid(computedEtagBytes);
            if(context.MatchEtag(computedEtag))
            {
                context.SetStatusToNotModified();
                return;
            }
		    context.Response.Headers["ETag"] = computedEtag.ToString();
			context.WriteJson(result);
		}
	}
}
