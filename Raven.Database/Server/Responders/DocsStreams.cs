using System.IO;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Server.Abstractions;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.Responders
{
	public class DocsStreams : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/streams/docs/?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			using (context.Response.Streaming())
			{
				context.Response.ContentType = "application/json; charset=utf-8";

				using (var writer = new JsonTextWriter(new StreamWriter(context.Response.OutputStream)))
				{
					writer.WriteStartObject();
					writer.WritePropertyName("Results");
					writer.WriteStartArray();

					Database.TransactionalStorage.Batch(accessor =>
					{
						var startsWith = context.Request.QueryString["startsWith"];
						int pageSize = context.GetPageSize(int.MaxValue);
						if (string.IsNullOrEmpty(context.Request.QueryString["pageSize"]))
							pageSize = int.MaxValue;

						// we may be sending a LOT of documents to the user, and most 
						// of them aren't going to be relevant for other ops, so we are going to skip
						// the cache for that, to avoid filling it up very quickly
						using (DocumentCacher.SkipSettingDocumentsInDocumentCache())
						{
							if (string.IsNullOrEmpty(startsWith))
							{
								Database.GetDocuments(context.GetStart(), pageSize, context.GetEtagFromQueryString(),
								                      doc => doc.WriteTo(writer));
							}
							else
							{
								Database.GetDocumentsWithIdStartingWith(
									startsWith,
									context.Request.QueryString["matches"],
                                    context.Request.QueryString["exclude"],
									context.GetStart(),
									pageSize,
									doc => doc.WriteTo(writer));
							}
						}
					});

					writer.WriteEndArray();
					writer.WriteEndObject();
					writer.Flush();
				}
			}
		}
	}
}