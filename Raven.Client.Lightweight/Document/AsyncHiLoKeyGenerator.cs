//-----------------------------------------------------------------------
// <copyright file="HiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET35 && !SILVERLIGHT
using System;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;

namespace Raven.Client.Document
{
	/// <summary>
	/// Generate hilo numbers against a RavenDB document
	/// </summary>
	public class AsyncHiLoKeyGenerator : HiLoKeyGeneratorBase
	{
		private readonly IAsyncDatabaseCommands databaseCommands;
		private SpinLock generatorLock = new SpinLock(enableThreadOwnerTracking: false); // Using a spin lock rather than Monitor.Enter, because it's not reentrant

		/// <summary>
		/// Initializes a new instance of the <see cref="HiLoKeyGenerator"/> class.
		/// </summary>
		public AsyncHiLoKeyGenerator(IAsyncDatabaseCommands databaseCommands, string tag, long capacity)
			: base(tag, capacity)
		{
			this.databaseCommands = databaseCommands;
		}

		/// <summary>
		/// Generates the document key.
		/// </summary>
		/// <param name="convention">The convention.</param>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
		public Task<string> GenerateDocumentKeyAsync(DocumentConvention convention, object entity)
		{
			return NextIdAsync().ContinueWith(task => GetDocumentKeyFromId(convention, task.Result));
		}

		///<summary>
		/// Create the next id (numeric)
		///</summary>
		public Task<long> NextIdAsync()
		{
			var myRange = range; // thread safe copy
			long incrementedCurrent = Interlocked.Increment(ref myRange.Current);
			if (incrementedCurrent <= myRange.Max)
			{
				return CompletedTask.With(incrementedCurrent);
			}

			bool lockTaken = false;
			try
			{
				generatorLock.Enter(ref lockTaken);
				if (range != myRange)
				{
					// Lock was contended, and the max has already been changed. Just get a new id as usual.
					generatorLock.Exit();
					return NextIdAsync();
				}
				else
				{
					// Get a new max, and use the current value.
					return GetNextRangeAsync()
						.ContinueWith(task =>
						{
							try
							{
								range = task.Result;
							}
							finally
							{
								generatorLock.Exit();
							}

							return NextIdAsync();
						}).Unwrap();
				}
			}
			catch
			{
				// We only unlock in exceptional cases (and not in a finally clause) because non exceptional cases will either have already
				// unlocked or will have started a task that will unlock in the future.
				if (lockTaken)
					generatorLock.Exit();
				throw;
			}
		}

		private Task<Range> GetNextRangeAsync()
		{
			IncreaseCapacityIfRequired();

			return GetNextMaxAsyncInner();
		}

		private Task<Range> GetNextMaxAsyncInner()
		{
			var minNextMax = range.Max;
			return GetDocumentAsync().ContinueWith(task =>
			{
				try
				{
					JsonDocument document;
					try
					{
						document = task.Result;
					}
					catch (ConflictException e)
					{
						// resolving the conflict by selecting the highest number
						var highestMax = e.ConflictedVersionIds
							.Select(conflictedVersionId => databaseCommands.GetAsync(conflictedVersionId)
									.ContinueWith(t => GetMaxFromDocument(t.Result, minNextMax)))
							.AggregateAsync(Enumerable.Max);

						return highestMax
							.ContinueWith(t => PutDocumentAsync(new JsonDocument
								{
									Etag = e.Etag,
									Metadata = new RavenJObject(),
									DataAsJson = RavenJObject.FromObject(new { Max = t.Result }),
									Key = HiLoDocumentKey
								}))
							.Unwrap()
							.ContinueWithTask(GetNextRangeAsync);
					}

					long min, max;
					if (document == null)
					{
						min = minNextMax + 1;
						max = minNextMax + capacity;
						document = new JsonDocument
						{
							Etag = Guid.Empty,
							// sending empty guid means - ensure the that the document does NOT exists
							Metadata = new RavenJObject(),
							DataAsJson = RavenJObject.FromObject(new { Max = max }),
							Key = HiLoDocumentKey
						};
					}
					else
					{
						var oldMax = GetMaxFromDocument(document, minNextMax);
						min = oldMax + 1;
						max = oldMax + capacity;

						document.DataAsJson["Max"] = max;
					}

					return PutDocumentAsync(document).WithResult(new Range(min, max));
				}
				catch (ConcurrencyException)
				{
					return GetNextMaxAsyncInner();
				}
			}).Unwrap();
		}

		private Task PutDocumentAsync(JsonDocument document)
		{
			return databaseCommands.PutAsync(HiLoDocumentKey, document.Etag,
								 document.DataAsJson,
								 document.Metadata);
		}

		private Task<JsonDocument> GetDocumentAsync()
		{
			return databaseCommands.GetAsync(new[] { HiLoDocumentKey, RavenKeyServerPrefix }, new string[0])
				.ContinueWith(task =>
				{
					var documents = task.Result;
					if (documents.Results.Count == 2 && documents.Results[1] != null)
					{
						lastServerPrefix = documents.Results[1].Value<string>("ServerPrefix");
					}
					else
					{
						lastServerPrefix = string.Empty;
					}
					if (documents.Results.Count == 0 || documents.Results[0] == null)
						return (JsonDocument)null;

					var jsonDocument = documents.Results[0].ToJsonDocument();
					foreach (var key in jsonDocument.Metadata.Keys.Where(x => x.StartsWith("@")).ToArray())
					{
						jsonDocument.Metadata.Remove(key);
					}
					return jsonDocument;
				});
		}
	}
}
#endif
