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
		public async Task<HttpResponseMessage> Create()
		{
			var subscriptionCriteria = await ReadJsonObjectAsync<SubscriptionCriteria>();

			var id = Database.Subscriptions.CreateSubscription(subscriptionCriteria);

			return GetMessageWithObject(new
			{
				SubscriptionId = id
			}, HttpStatusCode.Created);
		}

		[HttpPost]
		[Route("subscriptions/open")]
		[Route("databases/{databaseName}/subscriptions/open")]
		public async Task<HttpResponseMessage> Open(long id)
		{
			if (Database.Subscriptions.GetSubscriptionDocument(id) == null)
				return GetMessageWithString("Cannot find a subscription for the specified id: " + id, HttpStatusCode.NotFound);

			var options = await ReadJsonObjectAsync<SubscriptionBatchOptions>();

			if (options == null)
				throw new InvalidOperationException("Options cannot be null");

			string connectionId;

			if (Database.Subscriptions.TryOpenSubscription(id, options, out connectionId) == false)
				return GetMessageWithString("Subscription is already in use. There can be only a single open subscription connection per subscription.", HttpStatusCode.Gone);

			return GetMessageWithString(connectionId);
		}

		[HttpGet]
		[Route("subscriptions/pull")]
		[Route("databases/{databaseName}/subscriptions/pull")]
		public HttpResponseMessage Pull(long id, string connection)
		{
			Database.Subscriptions.AssertOpenSubscriptionConnection(id, connection);

			var pushStreamContent = new PushStreamContent((stream, content, transportContext) => StreamToClient(id, Database.Subscriptions, stream))
			{
				Headers =
				{
					ContentType = new MediaTypeHeaderValue("application/json")
					{
						CharSet = "utf-8"
					}
				}
			};

			return new HttpResponseMessage(HttpStatusCode.OK)
			{
				Content = pushStreamContent
			};
		}

		[HttpPost]
		[Route("subscriptions/acknowledgeBatch")]
		[Route("databases/{databaseName}/subscriptions/acknowledgeBatch")]
		public HttpResponseMessage AcknowledgeBatch(long id, string lastEtag, string connection)
		{
			Database.Subscriptions.AssertOpenSubscriptionConnection(id, connection);

			Database.Subscriptions.AcknowledgeBatchProcessed(id, Etag.Parse(lastEtag));

			return GetEmptyMessage();
		}

		[HttpPost]
		[Route("subscriptions/close")]
		[Route("databases/{databaseName}/subscriptions/close")]
		public HttpResponseMessage Close(long id, string connection)
		{
			Database.Subscriptions.AssertOpenSubscriptionConnection(id, connection);

			Database.Subscriptions.ReleaseSubscription(id);

			return GetEmptyMessage();
		}

		private void StreamToClient(long id, SubscriptionActions subscriptions, Stream stream)
		{
			using (var streamWriter = new StreamWriter(stream))
			using (var writer = new JsonTextWriter(streamWriter))
			{
				var options = subscriptions.GetBatchOptions(id);

				writer.WriteStartObject();
				writer.WritePropertyName("Results");
				writer.WriteStartArray();

				using (var cts = new CancellationTokenSource())
				using (var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout))
				{
					Etag lastProcessedDocEtag = null;
					var totalDocSize = 0;
					var totalDocCount = 0;
					bool hasMoreDocs = false;
					var startEtag = subscriptions.GetSubscriptionDocument(id).AckEtag;

					do
					{
						Database.TransactionalStorage.Batch(accessor =>
						{
							// we may be sending a LOT of documents to the user, and most 
							// of them aren't going to be relevant for other ops, so we are going to skip
							// the cache for that, to avoid filling it up very quickly
							using (DocumentCacher.SkipSettingDocumentsInDocumentCache())
							{
								Database.Documents.GetDocuments(-1, options.MaxDocCount - totalDocCount, startEtag, cts.Token, doc =>
								{
									timeout.Delay();

									if (options.MaxSize.HasValue && totalDocSize + doc.SerializedSizeOnDisk > options.MaxSize)
										return;

									lastProcessedDocEtag = doc.Etag;

									if (doc.Key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase))
										return;

									doc.ToJson().WriteTo(writer);
									writer.WriteRaw(Environment.NewLine);

									totalDocSize += doc.SerializedSizeOnDisk;
									totalDocCount++;
								});
							}

							if (lastProcessedDocEtag == null)
								hasMoreDocs = false;
							else
							{
								var lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
								hasMoreDocs = EtagUtil.IsGreaterThan(lastDocEtag, lastProcessedDocEtag);

								startEtag = lastProcessedDocEtag;
							}	
						});
					} while (hasMoreDocs && totalDocCount < options.MaxDocCount && (options.MaxSize.HasValue == false || totalDocSize < options.MaxSize));

					writer.WriteEndArray();

					if (lastProcessedDocEtag != null)
					{
						writer.WritePropertyName("LastProcessedEtag");
						writer.WriteValue(lastProcessedDocEtag.ToString());
					}

					writer.WriteEndObject();
					writer.Flush();
				}
			}
		}
	}
}