// -----------------------------------------------------------------------
//  <copyright file="Streams.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders
{
	public class Streams : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/streams/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET", "HEAD" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var index = match.Groups[1].Value;

			var query = context.GetIndexQueryFromHttpContext(int.MaxValue);
			if (string.IsNullOrEmpty(context.Request.QueryString["pageSize"]))
				query.PageSize = int.MaxValue;
			if (context.Request.HttpMethod == "HEAD")
				query.PageSize = 0;
			JsonWriter writer = null;
			Database.Query(index, query, information =>
			{
				context.Response.AddHeader("Raven-Result-Etag", information.ResultEtag.ToString());
				context.Response.AddHeader("Raven-Index-Etag", information.IndexEtag.ToString());
				context.Response.AddHeader("Raven-Is-Stale", information.IsStable ? "true" : "false");
				context.Response.AddHeader("Raven-Index", information.Index);
				context.Response.AddHeader("Raven-Total-Results", information.TotalResults.ToString(CultureInfo.InvariantCulture));
				context.Response.AddHeader("Raven-Index-Timestamp", information.IndexTimestamp.ToString(Default.DateTimeFormatsToWrite,CultureInfo.InvariantCulture));

				if (query.PageSize == 0)
					return;

				writer = new JsonTextWriter(new StreamWriter(context.Response.OutputStream));
				writer.WriteStartObject();
				writer.WritePropertyName("Results");
				writer.WriteStartArray();
			}, result => result.WriteTo(writer, Default.Converters));

			if (query.PageSize == 0)
				return;

			writer.WriteEndArray();
			writer.WriteEndObject();
			if (writer != null)
				writer.Close();

		}
	}
}