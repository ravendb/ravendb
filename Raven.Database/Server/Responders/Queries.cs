//-----------------------------------------------------------------------
// <copyright file="Queries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders
{
	public class Queries : AbstractRequestResponder
	{
	    private readonly static ILog log = LogManager.GetCurrentClassLogger();

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
		    if (context.Request.HttpMethod == "POST")
		    {
                long bytesRead;
		        itemsToLoad = context.ReadJsonArray(out bytesRead);
		        if (log.IsDebugEnabled)
		        {
                    log.Debug(string.Format("Loading {0:#,#;;0} ids, array size: {1:#,#;;0} bytes", itemsToLoad.Length, bytesRead));
		        }
            }
		    else
		    {
		        itemsToLoad = new RavenJArray(context.Request.QueryString.GetValues("id"));
            }

			var result = new MultiLoadResult();
			var loadedIds = new HashSet<string>();
			var includes = context.Request.QueryString.GetValues("include") ?? new string[0];
			var transformer = context.Request.QueryString["transformer"] ?? context.Request.QueryString["resultTransformer"];

		    var queryInputs = context.ExtractQueryInputs();
            
            var transactionInformation = GetRequestTransaction(context);
		    var includedEtags = new List<byte>();
			Database.TransactionalStorage.Batch(actions =>
			{
				foreach (RavenJToken item in itemsToLoad)
				{
					var value = item.Value<string>();
					if(loadedIds.Add(value)==false)
						continue;
					JsonDocument documentByKey = string.IsNullOrEmpty(transformer)
				                        ? Database.Get(value, transactionInformation)
                                        : Database.GetWithTransformer(value, transformer, transactionInformation, queryInputs);
				    if (documentByKey == null)
						continue;
					result.Results.Add(documentByKey.ToJson());

					if (documentByKey.Etag != null)
					{
						includedEtags.AddRange(documentByKey.Etag.ToByteArray());
					}
					includedEtags.Add((documentByKey.NonAuthoritativeInformation ?? false) ? (byte)0 : (byte)1);
				}

				var addIncludesCommand = new AddIncludesCommand(Database, transactionInformation, (etag, includedDoc) =>
				{
					includedEtags.AddRange(etag.ToByteArray());
					result.Includes.Add(includedDoc);
				}, includes, loadedIds);

				foreach (var item in result.Results.Where(item => item != null))
				{
					addIncludesCommand.Execute(item);
				}
			});

			Etag computedEtag;
            
			using (var md5 = MD5.Create())
			{
				var computeHash = md5.ComputeHash(includedEtags.ToArray());
				computedEtag = Etag.Parse(computeHash);
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
