//-----------------------------------------------------------------------
// <copyright file="HiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading;
using System.Transactions;
#if !NET35
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
#endif
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
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
		private readonly IDatabaseCommands databaseCommands;
		private readonly object generatorLock = new object();

		/// <summary>
		/// Initializes a new instance of the <see cref="HiLoKeyGenerator"/> class.
		/// </summary>
		public HiLoKeyGenerator(IDatabaseCommands databaseCommands, string tag, long capacity)
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
		public string GenerateDocumentKey(DocumentConvention convention, object entity)
		{
			return GetDocumentKeyFromId(convention, NextId());
		}

		///<summary>
		/// Create the next id (numeric)
		///</summary>
		public long NextId()
		{
			while (true)
			{
				long incrementedCurrent = Interlocked.Increment(ref current);
				if (incrementedCurrent <= currentMax.Value)
					return incrementedCurrent;

				lock (generatorLock)
				{
					if (current > currentMax.Value)
						currentMax = new LongHolder(GetNextMax());
				}
			}
		}

		private long GetNextMax()
		{
			using (new TransactionScope(TransactionScopeOption.Suppress))
			{
				IncreaseCapacityIfRequired();
				while (true)
				{
					try
					{
						var minNextMax = currentMax.Value;
						JsonDocument document;

						try
						{
							document = GetDocument();
						}
						catch (ConflictException e)
						{
							// resolving the conflict by selecting the highest number
							var highestMax = e.ConflictedVersionIds
								.Select(conflictedVersionId => GetMaxFromDocument(databaseCommands.Get(conflictedVersionId), minNextMax))
								.Max();

							PutDocument(new JsonDocument
							{
								Etag = e.Etag,
								Metadata = new RavenJObject(),
								DataAsJson = RavenJObject.FromObject(new { Max = highestMax }),
								Key = HiLoDocumentKey
							});

							continue;
						}
						if (document == null)
						{
							PutDocument(new JsonDocument
							{
								Etag = Guid.Empty,
								// sending empty guid means - ensure the that the document does NOT exists
								Metadata = new RavenJObject(),
								DataAsJson = RavenJObject.FromObject(new { Max = minNextMax + capacity }),
								Key = HiLoDocumentKey
							});
							return minNextMax + capacity;
						}
						var max = GetMaxFromDocument(document, minNextMax);
						document.DataAsJson["Max"] = max + capacity;
						PutDocument(document);

						current = max + 1;
						return max + capacity;
					}
					catch (ConcurrencyException)
					{
						// expected, we need to retry
					}
				}
			}
		}

		private void PutDocument(JsonDocument document)
		{
			databaseCommands.Put(HiLoDocumentKey, document.Etag,
								 document.DataAsJson,
								 document.Metadata);
		}

		private JsonDocument GetDocument()
		{
			var documents = databaseCommands.Get(new[] { HiLoDocumentKey, RavenKeyServerPrefix }, new string[0]);
			return HandleGetDocumentResult(documents);
		}
	}
}
