//-----------------------------------------------------------------------
// <copyright file="MoreLikeThisResponder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Specialized;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Queries;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Bundles.MoreLikeThis
{
	public class MoreLikeThisResponder : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/morelikethis/?(.+)"; } // /morelikethis/?index={index-name}&docid={ravendb-document-id}
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var parameters = GetParametersFromPath(context.GetRequestUrl(), context.Request.QueryString);

			var index = Database.IndexStorage.GetIndexInstance(parameters.IndexName);
			if (index == null)
			{
				context.SetStatusToNotFound();
				context.WriteJson(new { Error = "The index " + parameters.IndexName + " cannot be found" });
				return;
			}

			var indexEtag = Database.GetIndexEtag(parameters.IndexName, null);
			if (context.MatchEtag(indexEtag))
			{
				context.SetStatusToNotModified();
				return;
			}

			var result = Database.ExecuteMoreLikeThisQuery(parameters, GetRequestTransaction(context), context.GetPageSize(Database.Configuration.MaxPageSize), context.Request.QueryString.GetValues("include"));
			
			if (context.MatchEtag(result.Etag))
			{
				context.SetStatusToNotModified();
				return;
			}

			context.Response.AddHeader("ETag", result.Etag.ToString());
			context.WriteJson(result.Result);
		}

		public static MoreLikeThisQuery GetParametersFromPath(string path, NameValueCollection query)
		{
			var results = new MoreLikeThisQuery
			{
				IndexName = query.Get("index"),
				Fields = query.GetValues("fields"),
				Boost = query.Get("boost").ToNullableBool(),
				MaximumNumberOfTokensParsed = query.Get("maxNumTokens").ToNullableInt(),
				MaximumQueryTerms = query.Get("maxQueryTerms").ToNullableInt(),
				MaximumWordLength = query.Get("maxWordLen").ToNullableInt(),
				MinimumDocumentFrequency = query.Get("minDocFreq").ToNullableInt(),
				MinimumTermFrequency = query.Get("minTermFreq").ToNullableInt(),
				MinimumWordLength = query.Get("minWordLen").ToNullableInt(),
				StopWordsDocumentId = query.Get("stopWords"),
			};

			var keyValues = query.Get("docid").Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
			foreach (var keyValue in keyValues)
			{
				var split = keyValue.IndexOf('=');

				if (split >= 0)
				{
					results.MapGroupFields.Add(keyValue.Substring(0, split), keyValue.Substring(split + 1));
				}
				else
				{
					results.DocumentId = keyValue;
				}
			}

			return results;
		}
	}

	internal static class StringConverter
	{
		public static int? ToNullableInt(this string value)
		{
			int ret;
			if (value == null || !int.TryParse(value, out ret)) return null;
			return ret;
		}

		public static bool? ToNullableBool(this string value)
		{
			bool ret;
			if (value == null || !bool.TryParse(value, out ret)) return null;
			return ret;
		}
	}
}