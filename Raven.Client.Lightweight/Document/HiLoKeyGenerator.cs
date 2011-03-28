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
using Raven.Database;
using Raven.Database.Json;
using Raven.Http.Exceptions;
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
		private long currentHi;
		private long currentLo;

		/// <summary>
		/// Initializes a new instance of the <see cref="HiLoKeyGenerator"/> class.
		/// </summary>
		/// <param name="documentStore">The document store.</param>
		/// <param name="tag">The tag.</param>
		/// <param name="capacity">The capacity.</param>
		public HiLoKeyGenerator(IDocumentStore documentStore, string tag, long capacity)
		{
			currentHi = 0;
			this.documentStore = documentStore;
			this.tag = tag;
			this.capacity = capacity;
			currentLo = capacity + 1;
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

		private long NextId()
		{
			long incrementedCurrentLow = Interlocked.Increment(ref currentLo);
			if (incrementedCurrentLow > capacity)
			{
				lock (generatorLock)
				{
#if !SILVERLIGHT
					if (Thread.VolatileRead(ref currentLo) > capacity)
#else
					if (currentLo > capacity)
#endif
					{
						currentHi = GetNextHi();
						currentLo = 1;
						incrementedCurrentLow = 1;
					}
					else
					{
						incrementedCurrentLow = Interlocked.Increment(ref currentLo);
					}
				}
			}
			return (currentHi - 1) * capacity + (incrementedCurrentLow);
		}

#if !SILVERLIGHT
		private long GetNextHi()
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
									 RavenJObject.FromObject(new HiLoKey{ServerHi = 2}),
									 new RavenJObject());
						return 1;
					}
					var hiLoKey = document.DataAsJson.JsonDeserialization<HiLoKey>();
					var newHi = hiLoKey.ServerHi;
					hiLoKey.ServerHi += 1;
					databaseCommands.Put(RavenKeyGeneratorsHilo + tag, document.Etag,
								 RavenJObject.FromObject(hiLoKey),
								 document.Metadata);
					return newHi;
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

		#region Nested type: HiLoKey

		private class HiLoKey
		{
			public long ServerHi { get; set; }

		}

		#endregion
	}
}
