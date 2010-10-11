using System;
using System.Threading;
using System.Transactions;
using Newtonsoft.Json.Linq;
using Raven.Client.Client;
using Raven.Database.Exceptions;
using Raven.Database.Json;

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
        public string GenerateDocumentKey(DocumentConvention convention,object entity)
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
                    if (Thread.VolatileRead(ref currentLo) > capacity)
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
            return (currentHi - 1)*capacity + (incrementedCurrentLow);
        }

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
                                     JObject.FromObject(new HiLoKey{ServerHi = 2}),
                                     new JObject());
                        return 1;
                    }
                    var hiLoKey = document.DataAsJson.JsonDeserialization<HiLoKey>();
                    var newHi = hiLoKey.ServerHi;
                    hiLoKey.ServerHi += 1;
					databaseCommands.Put(RavenKeyGeneratorsHilo + tag, document.Etag,
                                 JObject.FromObject(hiLoKey),
                                 document.Metadata);
                    return newHi;
                }
                catch (ConcurrencyException)
                {
                   // expected, we need to retry
                }
            }
        }

        #region Nested type: HiLoKey

        private class HiLoKey
        {
            public long ServerHi { get; set; }

        }

        #endregion
    }
}
