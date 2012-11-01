//-----------------------------------------------------------------------
// <copyright file="HiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading;
using System.Transactions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Client.Connection;
using Raven.Client.Exceptions;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
	/// <summary>
	/// Generate hilo numbers against a RavenDB document
	/// </summary>
	public class HiLoKeyGenerator : HiLoKeyGeneratorBase
	{
		private readonly object generatorLock = new object();

		/// <summary>
		/// Initializes a new instance of the <see cref="HiLoKeyGenerator"/> class.
		/// </summary>
		public HiLoKeyGenerator(string tag, long capacity)
			: base(tag, capacity)
		{
		}

		/// <summary>
		/// Generates the document key.
		/// </summary>
		/// <param name="convention">The convention.</param>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
		public string GenerateDocumentKey(IDatabaseCommands databaseCommands, DocumentConvention convention, object entity)
		{
			return GetDocumentKeyFromId(convention, NextId(databaseCommands));
		}

		///<summary>
		/// Create the next id (numeric)
		///</summary>
		public long NextId(IDatabaseCommands commands)
		{
			while (true)
			{
				var myRange = Range; // thread safe copy

				var current = Interlocked.Increment(ref myRange.Current);
				if (current <= myRange.Max)
					return current;

				lock (generatorLock)
				{
					if (Range != myRange)
						// Lock was contended, and the max has already been changed. Just get a new id as usual.
						continue;

					Range = GetNextRange(commands);
				}
			}
		}

		private RangeValue GetNextRange(IDatabaseCommands databaseCommands)
		{
			using (new TransactionScope(TransactionScopeOption.Suppress))
			{
				IncreaseCapacityIfRequired();
				while (true)
				{
					try
					{
						var minNextMax = Range.Max;
						JsonDocument document;

						try
						{
							document = GetDocument(databaseCommands);
						}
						catch (ConflictException e)
						{
							// resolving the conflict by selecting the highest number
							var highestMax = e.ConflictedVersionIds
								.Select(conflictedVersionId => GetMaxFromDocument(databaseCommands.Get(conflictedVersionId), minNextMax))
								.Max();

							PutDocument(databaseCommands, new JsonDocument
							{
								Etag = e.Etag,
								Metadata = new RavenJObject(),
								DataAsJson = RavenJObject.FromObject(new { Max = highestMax }),
								Key = HiLoDocumentKey
							});

							continue;
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
						PutDocument(databaseCommands, document);

						return new RangeValue(min, max);
					}
					catch (ConcurrencyException)
					{
						// expected, we need to retry
					}
				}
			}
		}

		private void PutDocument(IDatabaseCommands databaseCommands,JsonDocument document)
		{
			databaseCommands.Put(HiLoDocumentKey, document.Etag,
								 document.DataAsJson,
								 document.Metadata);
		}

		private JsonDocument GetDocument(IDatabaseCommands databaseCommands)
		{
			var documents = databaseCommands.Get(new[] { HiLoDocumentKey, RavenKeyServerPrefix }, new string[0]);
			return HandleGetDocumentResult(documents);
		}
	}
}
