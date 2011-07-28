//-----------------------------------------------------------------------
// <copyright file="HiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading;
#if !SILVERLIGHT
using System.Transactions;
#endif
#if !NET_3_5
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
#endif
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
	/// <summary>
	/// Generate hilo numbers against a RavenDB document
	/// </summary>
	public class HiLoKeyGenerator
	{
		private const string RavenKeyGeneratorsHilo = "Raven/Hilo/";
		private readonly IDocumentStore documentStore;
		private readonly string tag;
		private readonly long capacity;
		private readonly object generatorLock = new object();
		private long current;
		private volatile Hodler currentMax = new Hodler(0);

		private class Hodler
		{
			public readonly long Value;

			public Hodler(long value)
			{
				Value = value;
			}
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="HiLoKeyGenerator"/> class.
		/// </summary>
		/// <param name="documentStore">The document store.</param>
		/// <param name="tag">The tag.</param>
		/// <param name="capacity">The capacity.</param>
		public HiLoKeyGenerator(IDocumentStore documentStore, string tag, long capacity)
		{
			this.documentStore = documentStore;
			this.tag = tag;
			this.capacity = capacity;
			current = 0;
		}

		/// <summary>
		/// Generates the document key.
		/// </summary>
		/// <param name="convention">The convention.</param>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
		public string GenerateDocumentKey(DocumentConvention convention, object entity)
		{
			return string.Format("{0}{1}{2}",
								 tag,
								 convention.IdentityPartsSeparator,
								 NextId());
		}

		///<summary>
		/// Create the next id (numeric)
		///</summary>
		public long NextId()
		{
			long incrementedCurrent = Interlocked.Increment(ref current);
			while (incrementedCurrent > currentMax.Value)
			{
				lock (generatorLock)
				{
					if (current > currentMax.Value)
					{
						currentMax = new Hodler(GetNextMax());
						incrementedCurrent = current;
					}
					else
					{
						incrementedCurrent = Interlocked.Increment(ref current);
					}
				}
			}
			return incrementedCurrent;
		}

		private long GetNextMax()
		{
#if !SILVERLIGHT
			using (new TransactionScope(TransactionScopeOption.Suppress))
#endif
			while (true)
			{
				try
				{
					var document = GetDocument();
					if (document == null)
					{
						PutDocument(new JsonDocument
						{
							Etag = Guid.Empty, // sending empty guid means - ensure the that the document does NOT exists
							Metadata = new RavenJObject(),
							DataAsJson = RavenJObject.FromObject(new {Max = capacity}),
							Key = RavenKeyGeneratorsHilo + tag
						});
						return capacity;
					}
					long max;
					if (document.DataAsJson.ContainsKey("ServerHi")) // convert from hi to max
					{
						var hi = document.DataAsJson.Value<long>("ServerHi");
						max = ((hi- 1) * capacity);
						document.DataAsJson.Remove("ServerHi");
						document.DataAsJson["Max"] = max;
					}
					max = document.DataAsJson.Value<long>("Max");
					document.DataAsJson["Max"] = max + capacity;
					PutDocument(document);

					current = max+1;
					return max + capacity;
				}
				catch (ConcurrencyException)
				{
				   // expected, we need to retry
				}
			}
		}

#if !SILVERLIGHT
		private void PutDocument(JsonDocument document)
		{
			documentStore.DatabaseCommands.Put(RavenKeyGeneratorsHilo + tag, document.Etag,
			                     document.DataAsJson,
			                     document.Metadata);
		}

		private JsonDocument GetDocument()
		{
			return documentStore.DatabaseCommands.Get(RavenKeyGeneratorsHilo + tag);
		}
#else
		private void PutDocument(JsonDocument document)
		{
			documentStore.AsyncDatabaseCommands.PutAsync(RavenKeyGeneratorsHilo + tag, document.Etag,
			                     document.DataAsJson,
			                     document.Metadata)
					.Wait();
		}

		private JsonDocument GetDocument()
		{
			return documentStore.AsyncDatabaseCommands.GetAsync(RavenKeyGeneratorsHilo + tag).Result;
		}
#endif

	}
}
