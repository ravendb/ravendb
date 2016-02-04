using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Document;

namespace Raven.Monitor
{
    public static class RavenDocumentStore
    {
        private static IDocumentStore store;

        public static bool Init(String url)
        {
            lock (Locker)
            {
                if (store == null)
                {
                    try
                    {
                        store = new DocumentStore() {Url = url}.Initialize();
                    }
                    catch
                    {
                        store = null;
                        return false;
                    }					
                }
            }
            return true;
        }
        public static IDocumentStore DocumentStore
        {
            get
            {
                if (store != null)
                    return store;
                throw new NullReferenceException("Document store is not initialized");
            }
        }

        private static readonly object Locker = new object();
    }
}
