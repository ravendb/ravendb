using System;
using System.Threading;

namespace Raven.Client.Document
{
    public class HiLoKeyGenerator
    {
        private readonly long capacity;
        private readonly object generatorLock;
        private long currentHi;
        private long currentLo;

        public HiLoKeyGenerator(long capacity)
        {
            generatorLock = new object();
            currentHi = 0;
            this.capacity = capacity;
            currentLo = capacity + 1;
        }

        public string GenerateDocumentKey(DocumentConvention conventions, object entity)
        {
            // We allow the server to assign hi lo's key!
            if (entity is HiLoKey)
            {
                return "hilo/";
            }

            // Or we assign one ourselves using HiLo
            return string.Format("{0}/{1}",
                                 conventions.GetTypeTagName(entity.GetType()).ToLowerInvariant(),
                                 NextId());
        }

        private long NextId()
        {
            long incrementedCurrentLow = Interlocked.Increment(ref currentLo);
            if (incrementedCurrentLow >= capacity)
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
            return (currentHi - 1)*capacity + (incrementedCurrentLow);
        }

        private long GetNextHi()
        {
            using (IDocumentSession session = documentStore.OpenSession())
            {
                // Dump a new object into the db
                var store = new HiLoKey {Timestamp = DateTime.Now.ToString()};
                session.Store(store);
                session.SaveChanges();

                // And use its generated id as our new Hi value
                return Convert.ToInt64(store.Id.Split('/')[1]);
            }
        }

        #region Nested type: HiLoKey

        private class HiLoKey
        {
            public string Id { get; set; }

            public string Timestamp { get; set; }
        }

        #endregion
    }
}