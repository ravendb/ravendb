// -----------------------------------------------------------------------
//  <copyright file="Subscription.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
	public class Subscription : IObservable<JsonDocument>, IDisposable
	{
		private readonly SubscriptionBatchOptions options;
		private readonly IAsyncDatabaseCommands commands;
		private readonly ConcurrentSet<IObserver<JsonDocument>> subscribers = new ConcurrentSet<IObserver<JsonDocument>>();
		private bool open;
		private bool disposed = false;
		private CancellationTokenSource cts = new CancellationTokenSource();

		public Subscription(string name, SubscriptionBatchOptions options, IAsyncDatabaseCommands commands)
		{
			Name = name;
			this.options = options;
			this.commands = commands;
		}

		public string Name { get; private set; }

		public Task Task { get; private set; }

		private void Open()
		{
			Task = OpenConnection();
			//.ObserveException()
			//.ContinueWith(task => task.AssertNotFailed());

			open = true;
		}

		private async Task OpenConnection()
		{
			if (string.IsNullOrEmpty(Name))
				throw new InvalidOperationException("Subscription does not have any name");

			var subscriptionRequest = commands.CreateRequest("/subscriptions/open?name=" + Name, "POST", disableRequestCompression: true, disableAuthentication: true);

			using (var response = await subscriptionRequest.ExecuteRawResponseAsync(RavenJObject.FromObject(options)).ConfigureAwait(false))
			{
				await response.AssertNotFailingResponse().ConfigureAwait(false);

				using (var responseStream = await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false))
				using (var streamReader = new StreamReader(responseStream))
				{
					while (true)
					{
						cts.Token.ThrowIfCancellationRequested();

						var jsonReader = new JsonTextReaderAsync(streamReader);

						var type = await TryReadType(jsonReader).ConfigureAwait(false);

						if (type == null)
							continue;

						switch (type)
						{
							case "Data":
								using (var streamedDocs = new AsyncServerClient.YieldStreamResults(subscriptionRequest, responseStream, jsonReader: jsonReader, batchReadingMode: true))
								{
									while (await streamedDocs.MoveNextAsync().ConfigureAwait(false))
									{
										cts.Token.ThrowIfCancellationRequested();

										var jsonDoc = SerializationHelper.RavenJObjectToJsonDocument(streamedDocs.Current);
										foreach (var subscriber in subscribers)
										{
											subscriber.OnNext(jsonDoc);
										}
									}
								}

								var lastProcessedEtagInBatch = await ReadLastProcessedEtag(jsonReader).ConfigureAwait(false);

								await EnsureValidEndOfMessage(jsonReader, streamReader).ConfigureAwait(false);

								using (var acknowledgmentRequest = commands.CreateRequest(string.Format("/subscriptions/acknowledgeBatch?name={0}&lastEtag={1}", Name, lastProcessedEtagInBatch), "POST"))
								{
									acknowledgmentRequest.ExecuteRequest();
								}

								break;
							case "Heartbeat":
								await EnsureValidEndOfMessage(jsonReader, streamReader).ConfigureAwait(false);
								break;
							default:
								throw new InvalidOperationException("Unknown type of stream part: " + type);
						}
					}
				}
			}
		}

		private static async Task EnsureValidEndOfMessage(JsonTextReaderAsync reader, StreamReader streamReader)
		{
			if (reader.TokenType != JsonToken.EndObject && await reader.ReadAsync().ConfigureAwait(false) == false)
				throw new InvalidOperationException("Unexpected end of message - missing EndObject token");

			if (reader.TokenType != JsonToken.EndObject)
				throw new InvalidOperationException(string.Format("Unexpected token type at the end of the message: {0}. Error: {1}", reader.TokenType, streamReader.ReadToEnd()));
		}

		private static async Task<string> TryReadType(JsonTextReaderAsync reader)
		{
			if (await reader.ReadAsync().ConfigureAwait(false) == false || reader.TokenType == JsonToken.None)
				return null;

			if (reader.TokenType != JsonToken.StartObject)
				throw new InvalidOperationException("Invalid subscription stream format. Unexpected toke type:" + reader.TokenType);

			if (await reader.ReadAsync().ConfigureAwait(false) && reader.TokenType != JsonToken.PropertyName)
				throw new InvalidOperationException("Invalid subscription stream format. Unexpected toke type:" + reader.TokenType);

			if (Equals("Type", reader.Value) == false)
				throw new InvalidOperationException("Unexpected property name. Got: '" + reader.TokenType + "' instead of 'Type'");

			return await reader.ReadAsString().ConfigureAwait(false);
		}

		private static async Task<Etag> ReadLastProcessedEtag(JsonTextReaderAsync reader)
		{
			if (Equals("LastProcessedEtag", reader.Value) == false)
				throw new InvalidOperationException("Unexpected property name. Got: '" + reader.TokenType + "' instead of 'LastProcessedEtag'");

			return Etag.Parse(await reader.ReadAsString().ConfigureAwait(false));
		}

		public IDisposable Subscribe(IObserver<JsonDocument> observer)
		{
			if (subscribers.TryAdd(observer))
			{
				if (!open)
					Open();
			}

			return new DisposableAction(() => subscribers.TryRemove(observer));
		}

		public void Dispose()
		{
			if (disposed)
				return;

			DisposeAsync().Wait();
		}

		public Task DisposeAsync()
		{
			if (disposed)
				return new CompletedTask();
			disposed = true;

			foreach (var subscriber in subscribers)
			{
				subscriber.OnCompleted();
			}

			if (!open) 
				return new CompletedTask();

			cts.Cancel();

			switch (Task.Status)
			{
				case TaskStatus.RanToCompletion:
				case TaskStatus.Canceled:
					break;
				default:
					try
					{
						Task.Wait();
					}
					catch (AggregateException ae)
					{
						if (ae.InnerException is OperationCanceledException == false)
						{
							throw;
						}
					}

					break;
			}

			using (var closeRequest = commands.CreateRequest(string.Format("/subscriptions/close?name={0}", Name), "POST"))
			{
				return closeRequest.ExecuteRequestAsync();
			}
		}
	}
}