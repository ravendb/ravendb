using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Raven.Client.Document
{
    public class HiLoKeyGenerator
    {
        private object generatorLock;
        private IDocumentStore documentStore;
        private long capacity;
        private long currentLo;
        private long currentHi;

        public HiLoKeyGenerator(long capacity, IDocumentStore documentStore)
        {
            this.generatorLock = new object();
            this.documentStore = documentStore;
            this.currentHi = 0;
            this.capacity = capacity;
            this.currentLo = capacity + 1;
        }

        public void SetupConventions(DocumentConvention conventions)
        {
            conventions.DocumentKeyGenerator = (Object entity) => GenerateDocumentKey(conventions, entity);
        }

        public string GenerateDocumentKey(DocumentConvention conventions,  object entity)
        {
            // We allow the server to assign hi lo's key!
            if (entity is HiLoKey) { return "hilo/"; }

            // Or we assign one ourselves using HiLo
            return string.Format("{0}/{1}",
                    conventions.GetTypeTagName(entity.GetType()).ToLowerInvariant(),
                    NextId());
        }

        private long NextId()
        {
            var incrementedCurrentLow = Interlocked.Increment(ref currentLo);
            if(incrementedCurrentLow >= capacity)
            {
                lock (generatorLock)
                {
                    if (Thread.VolatileRead(ref currentLo) >= capacity)
                    {
                        currentHi = GetNextHi();
                        currentLo = 0;
                        incrementedCurrentLow = 0;
                    }
                }
            }
            return (currentHi - 1) * capacity + (incrementedCurrentLow);
        }

        private long GetNextHi()
        {
            using (var session = documentStore.OpenSession())
            {
                // Dump a new object into the db
                var store = new HiLoKey {Timestamp = DateTime.Now.ToString()};
                session.Store(store);
                session.SaveChanges();

                // And use its generated id as our new Hi value
                return Convert.ToInt64(store.Id.Split('/')[1]);
            }
        }


        private class HiLoKey
        {
            public string Id
            {
                get;
                set;
            }

            public string Timestamp
            {
                get;set;
            }
        }
    }
}
