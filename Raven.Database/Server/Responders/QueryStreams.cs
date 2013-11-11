// -----------------------------------------------------------------------
//  <copyright file="Streams.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Abstractions;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Server.Abstractions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders
{
	public class QueryStreams : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/streams/query/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET", "HEAD" }; }
		}

		public override void Respond(IHttpContext context)
		{
			using (context.Response.Streaming())
			{
				context.Response.ContentType = "application/json; charset=utf-8";

				var match = urlMatcher.Match(context.GetRequestUrl());
				var index = match.Groups[1].Value;

				var query = context.GetIndexQueryFromHttpContext(int.MaxValue);
				if (string.IsNullOrEmpty(context.Request.QueryString["pageSize"]))
					query.PageSize = int.MaxValue;
				var isHeadRequest = context.Request.HttpMethod == "HEAD";
				if (isHeadRequest)
					query.PageSize = 0;

				using (var writer = GetOutputWriter(context))
				{
					// we may be sending a LOT of documents to the user, and most 
					// of them aren't going to be relevant for other ops, so we are going to skip
					// the cache for that, to avoid filling it up very quickly
					using (DocumentCacher.SkipSettingDocumentsInDocumentCache())
					{
						try
						{
							Database.Query(index, query, information =>
							{
								context.Response.AddHeader("Raven-Result-Etag", information.ResultEtag.ToString());
								context.Response.AddHeader("Raven-Index-Etag", information.IndexEtag.ToString());
								context.Response.AddHeader("Raven-Is-Stale", information.IsStable ? "true" : "false");
								context.Response.AddHeader("Raven-Index", information.Index);
								context.Response.AddHeader("Raven-Total-Results",
									information.TotalResults.ToString(CultureInfo.InvariantCulture));
								context.Response.AddHeader("Raven-Index-Timestamp",
									information.IndexTimestamp.ToString(Default.DateTimeFormatsToWrite,
										CultureInfo.InvariantCulture));

								if (isHeadRequest)
									return;
								writer.WriteHeader();
							}, writer.Write);
						}
						catch (IndexDoesNotExistsException e)
						{
							if (index.StartsWith("dynamic/", StringComparison.InvariantCultureIgnoreCase))
							{
								throw new NotSupportedException(@"StreamQuery() does not support querying dynamic indexes. It is designed to be used with large data-sets and is unlikely to return all data-set after 15 sec of indexing, like Query() does.", e);
							}

							throw;						
						}
					}
				}
			}
		}

		private static IOutputWriter GetOutputWriter(IHttpContext context)
		{
			var useExcelFormat = "excel".Equals(context.Request.QueryString["format"], StringComparison.InvariantCultureIgnoreCase);
			if (useExcelFormat)
				return new ExcelOutputWriter(context);
			return new JsonOutputWriter(context);
		}

		public interface IOutputWriter : IDisposable
		{
			void WriteHeader();
			void Write(RavenJObject result);
		}

		private class ExcelOutputWriter : IOutputWriter
		{

			private readonly IHttpContext context;
			private StreamWriter writer;

			public ExcelOutputWriter(IHttpContext context)
			{
				this.context = context;
				context.Response.ContentType = "text/csv, application/vnd.msexcel, text/anytext";
			}

			public void Dispose()
			{
				if (writer == null)
					return;

				writer.Flush();
				writer.Close();
			}

			public void WriteHeader()
			{
				writer = new StreamWriter(context.Response.OutputStream, Encoding.UTF8);
			}

			public void Write(RavenJObject result)
			{
				if (properties == null)
				{
					GetPropertiesAndWriteCsvHeader(result);
					Debug.Assert(properties != null);
				}

				foreach (var property in properties)
				{
					var token = result.SelectToken(property);
					if (token != null)
					{
						switch (token.Type)
						{
							case JTokenType.Null:
								break;

							case JTokenType.Array:
							case JTokenType.Object:
								OutputCsvValue(token.ToString(Formatting.None));
								break;

							default:
								OutputCsvValue(token.Value<string>());
								break;
						}
					}

					writer.Write(',');
				}

				writer.WriteLine();
			}

			private void GetPropertiesAndWriteCsvHeader(RavenJObject result)
			{
				properties = DocumentHelpers.GetPropertiesFromJObject(result,
																	  parentPropertyPath: "",
																	  includeNestedProperties: true,
																	  includeMetadata: false,
																	  excludeParentPropertyNames: true);

				foreach (var property in properties)
				{
					OutputCsvValue(property);
					writer.Write(',');
				}
				writer.WriteLine();
			}

			static readonly char[] requireQuotesChars = new[] { ',', '\r', '\n', '"' };
			private IEnumerable<string> properties;

			private void OutputCsvValue(string val)
			{
				var needsQuoutes = val.IndexOfAny(requireQuotesChars) != -1;
				if (needsQuoutes)
				{
					writer.Write('"');
				}
				writer.Write(needsQuoutes ? val.Replace("\"", "\"\"") : val);
				if (needsQuoutes)
					writer.Write('"');
			}
		}

		public class JsonOutputWriter : IOutputWriter
		{
			private readonly IHttpContext context;
			private JsonWriter writer;

			public JsonOutputWriter(IHttpContext context)
			{
				this.context = context;
			}

			public void WriteHeader()
			{
				writer = new JsonTextWriter(new StreamWriter(context.Response.OutputStream));
				writer.WriteStartObject();
				writer.WritePropertyName("Results");
				writer.WriteStartArray();
			}

			public void Dispose()
			{
				if (writer == null)
					return;

				writer.WriteEndArray();
				writer.WriteEndObject();

				writer.Flush();
				writer.Close();
			}

			public void Write(RavenJObject result)
			{
				result.WriteTo(writer, Default.Converters);
			}
		}
	}
}