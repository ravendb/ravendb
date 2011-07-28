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
#endif
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
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

#if !SILVERLIGHT
		private long GetNextMax()
		{
			using(new TransactionScope(TransactionScopeOption.Suppress))
			while (true)
			{
				try
				{
					var databaseCommands = documentStore.DatabaseCommands;
					var document = databaseCommands.Get(RavenKeyGeneratorsHilo + tag);
					if (document == null)
					{
						databaseCommands.Put(RavenKeyGeneratorsHilo + tag,
									 Guid.Empty,
									 // sending empty guid means - ensure the that the document does NOT exists
									 RavenJObject.FromObject(new {Max = capacity}),
									 new RavenJObject());
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
					databaseCommands.Put(RavenKeyGeneratorsHilo + tag, document.Etag,
								 document.DataAsJson,
								 document.Metadata);

					current = max+1;
					return max + capacity;
				}
				catch (ConcurrencyException)
				{
				   // expected, we need to retry
				}
			}
		}
#else
		private long GetNextHi()
		{
			while (true)
			{
				try
				{
					var databaseCommands = documentStore.AsyncDatabaseCommands;
					var docTask = databaseCommands.GetAsync(RavenKeyGeneratorsHilo + tag);
					docTask.Wait();
					var document = docTask.Result;
					if (document == null)
					{
						databaseCommands.PutAsync(RavenKeyGeneratorsHilo + tag,
										Guid.Empty, // sending empty guid means - ensure the that the document does NOT exists
										RavenJObject.FromObject(new HiLoKey { ServerHi = 2 }),
										new RavenJObject())
										.Wait();
						return 1;
					}
					var hiLoKey = document.DataAsJson.JsonDeserialization<HiLoKey>();
					var newHi = hiLoKey.ServerHi;
					hiLoKey.ServerHi += 1;
					databaseCommands.PutAsync(RavenKeyGeneratorsHilo + tag, document.Etag,
					                          RavenJObject.FromObject(hiLoKey),
					                          document.Metadata)
						.Wait();
					return newHi;
				}
				catch (ConcurrencyException)
				{
					// expected, we need to retry
				}
			}
		}

#endif
	}
}
