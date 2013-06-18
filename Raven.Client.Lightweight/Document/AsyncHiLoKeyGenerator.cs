//-----------------------------------------------------------------------
// <copyright file="HiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !SILVERLIGHT
using System;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
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
		private SpinLock generatorLock = new SpinLock(enableThreadOwnerTracking: false); // Using a spin lock rather than Monitor.Enter, because it's not reentrant

		/// <summary>
		/// Initializes a new instance of the <see cref="HiLoKeyGenerator"/> class.
		/// </summary>
		public AsyncHiLoKeyGenerator(string tag, long capacity)
			: base(tag, capacity)
		{
		}

		/// <summary>
		/// Generates the document key.
		/// </summary>
		/// <param name="convention">The convention.</param>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
		public async Task<string> GenerateDocumentKeyAsync(IAsyncDatabaseCommands databaseCommands, DocumentConvention convention, object entity)
		{
			var nextId = await NextIdAsync(databaseCommands);
			return GetDocumentKeyFromId(convention, nextId);
		}

		///<summary>
		/// Create the next id (numeric)
		///</summary>
		public async Task<long> NextIdAsync(IAsyncDatabaseCommands databaseCommands)
		{
			var myRange = Range; // thread safe copy
			long incrementedCurrent = Interlocked.Increment(ref myRange.Current);
			if (incrementedCurrent <= myRange.Max)
			{
				return incrementedCurrent;
			}

			bool lockTaken = false;
			try
			{
				generatorLock.Enter(ref lockTaken);
				if (Range != myRange)
				{
					// Lock was contended, and the max has already been changed. Just get a new id as usual.
					generatorLock.Exit();
					return await NextIdAsync(databaseCommands);
				}
				// Get a new max, and use the current value.

				try
				{
					Range = await GetNextRangeAsync(databaseCommands);
				}
				finally
				{
					generatorLock.Exit();
				}

				return await NextIdAsync(databaseCommands);
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

		private Task<RangeValue> GetNextRangeAsync(IAsyncDatabaseCommands databaseCommands)
		{
			ModifyCapacityIfRequired();

			return GetNextMaxAsyncInner(databaseCommands);
		}

		private async Task<RangeValue> GetNextMaxAsyncInner(IAsyncDatabaseCommands databaseCommands)
		{
			var minNextMax = Range.Max;
			try
			{
				JsonDocument document = null;
				ConflictException conflictException = null;
				try
				{
					document = await GetDocumentAsync(databaseCommands);
				}
				catch (ConflictException e)
				{
					conflictException = e;
				}
				if (conflictException != null)
				{
					// resolving the conflict by selecting the highest number
					var highestMax = conflictException.ConflictedVersionIds
					                                  .Select(async conflictedVersionId =>
					                                  {
						                                  var doc = await databaseCommands.GetAsync(conflictedVersionId);
						                                  return GetMaxFromDocument(doc, minNextMax);
					                                  })
					                                  .AggregateAsync(Enumerable.Max);

					await PutDocumentAsync(databaseCommands, new JsonDocument
					{
						Etag = conflictException.Etag,
						Metadata = new RavenJObject(),
						DataAsJson = RavenJObject.FromObject(new {Max = highestMax}),
						Key = HiLoDocumentKey
					});

					return await GetNextRangeAsync(databaseCommands);
				}

				long min, max;
				if (document == null)
				{
					min = minNextMax + 1;
					max = minNextMax + capacity;
					document = new JsonDocument
					{
						Etag = Etag.Empty,
						// sending empty etag means - ensure the that the document does NOT exists
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

				return await PutDocumentAsync(databaseCommands, document).WithResult(new RangeValue(min, max));
			}
			catch (ConcurrencyException)
			{
				// We will retry the operation. Since we have here just one catch clause, 
				// we can just continue to the next line without using a flag like retry = true
			}
			return await GetNextMaxAsyncInner(databaseCommands);
		}

		private Task PutDocumentAsync(IAsyncDatabaseCommands databaseCommands, JsonDocument document)
		{
			return databaseCommands.PutAsync(HiLoDocumentKey, document.Etag, document.DataAsJson, document.Metadata);
		}

		private async Task<JsonDocument> GetDocumentAsync(IAsyncDatabaseCommands databaseCommands)
		{
			var documents = await databaseCommands.GetAsync(new[] {HiLoDocumentKey, RavenKeyServerPrefix}, new string[0]);
			if (documents.Results.Count == 2 && documents.Results[1] != null)
			{
				lastServerPrefix = documents.Results[1].Value<string>("ServerPrefix");
			}
			else
			{
				lastServerPrefix = string.Empty;
			}
			if (documents.Results.Count == 0 || documents.Results[0] == null)
				return null;

			var jsonDocument = documents.Results[0].ToJsonDocument();
			foreach (var key in jsonDocument.Metadata.Keys.Where(x => x.StartsWith("@")).ToArray())
			{
				jsonDocument.Metadata.Remove(key);
			}
			return jsonDocument;
		}
	}
}
#endif