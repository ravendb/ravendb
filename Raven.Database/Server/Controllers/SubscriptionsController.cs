// -----------------------------------------------------------------------
//  <copyright file="SubscriptionsController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Actions;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.Controllers
{
	public class SubscriptionsController : RavenDbApiController
	{
		[HttpPost]
		[Route("subscriptions/create")]
		[Route("databases/{databaseName}/subscriptions/create")]
		public async Task<HttpResponseMessage> Create(string name)
		{
			var subscriptionCriteria = await ReadJsonObjectAsync<SubscriptionCriteria>();

			Database.Subscriptions.CreateSubscription(name, subscriptionCriteria);

			return GetEmptyMessage();
		}

		[HttpPost]
		[Route("subscriptions/open")]
		[Route("databases/{databaseName}/subscriptions/open")]
		public async Task<HttpResponseMessage> Open(string name)
		{
			var options = await ReadJsonObjectAsync<SubscriptionBatchOptions>();

			if(options == null)
				throw new InvalidOperationException("Options cannot be null");

			var addConnection = Database.Subscriptions.OpenSubscription(name);

			var pushStreamContent = new PushStreamContent((stream, content, transportContext) => StreamToClient(name, Database.Subscriptions, stream, options))
			{
				Headers =
				{
					ContentType = new MediaTypeHeaderValue("text/event-stream")
					{
						CharSet = "utf-8"
					}
				}
			};

			addConnection(pushStreamContent);

			return new HttpResponseMessage(HttpStatusCode.OK)
			{
				Content = pushStreamContent
			};
		}

		[HttpPost]
		[Route("subscriptions/close")]
		[Route("databases/{databaseName}/subscriptions/close")]
		public HttpResponseMessage Close(string name)
		{
			Database.Subscriptions.ReleaseSubscription(name);

			return GetEmptyMessage();
		}

		private void StreamToClient(string name, SubscriptionActions subscriptions, Stream stream, SubscriptionBatchOptions options)
		{
			var start = -1; // ignored

			using (var streamWriter = new StreamWriter(stream))
			using (var writer = new JsonTextWriter(streamWriter))
			{
				while (true)
				{
					using (var cts = new CancellationTokenSource())
					using (var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout))
					{
						writer.WriteStartObject();
						writer.WritePropertyName("Type");
						writer.WriteValue("Data");
						writer.WritePropertyName("Results");
						writer.WriteStartArray();

						var lastProcessedDocEtag = Etag.Empty;
						var totalSizeOfDocs = 0;

						Database.TransactionalStorage.Batch(accessor =>
						{
							var ackEtag = subscriptions.GetSubscriptionDocument(name).AckEtag;

							// we may be sending a LOT of documents to the user, and most 
							// of them aren't going to be relevant for other ops, so we are going to skip
							// the cache for that, to avoid filling it up very quickly
							using (DocumentCacher.SkipSettingDocumentsInDocumentCache())
							{
								Database.Documents.GetDocuments(start, options.MaxDocCount, ackEtag, cts.Token, doc =>
								{
									timeout.Delay();

									if (totalSizeOfDocs > options.MaxSize)
										return;

									lastProcessedDocEtag = doc.Etag;
									
									if (doc.Key.StartsWith("Raven/"))
										return;

									doc.ToJson().WriteTo(writer);
									writer.WriteRaw(Environment.NewLine);

									totalSizeOfDocs += doc.SerializedSizeOnDisk;
								});
							}
						});

						writer.WriteEndArray();
						writer.WritePropertyName("LastProcessedEtag");
						writer.WriteValue(lastProcessedDocEtag.ToString());
						writer.WriteEndObject();
						writer.Flush();
					}

					var batchAcknowledged = subscriptions.WaitForAcknowledgement(name, options.AcknowledgementTimeout);

					// TODO arek - cleanup this mess
					if (subscriptions.IsClosed(name))
						break;

					if(batchAcknowledged == false)
						continue;

					var lastDocEtag = Etag.Empty;

					Database.TransactionalStorage.Batch(accessor => lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag());

					var lastAckEtag = subscriptions.GetSubscriptionDocument(name).AckEtag;

					if(EtagUtil.IsGreaterThan(lastDocEtag, lastAckEtag))
						continue;

					subscriptions.WaitForNewDocuments(name);

					if (subscriptions.IsClosed(name))
						break;

				}
			}
		}

		[HttpPost]
		[Route("subscriptions/acknowledgeBatch")]
		[Route("databases/{databaseName}/subscriptions/acknowledgeBatch")]
		public HttpResponseMessage AcknowledgeBatch(string name, string lastEtag)
		{
			Database.Subscriptions.AcknowledgeBatchProcessed(name, Etag.Parse(lastEtag));

			return GetEmptyMessage();
		}
	}
}