using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Kayak;
using Rhino.DivanDB.Server.Responders;

namespace Rhino.DivanDB.Server
{
    public class DivanServer : IDisposable
    {
        private readonly DocumentDatabase database;
        private readonly KayakServer server;

        public DivanServer(string directory, int port)
        {
            database = new DocumentDatabase(directory);
            database.SpinBackgroundWorkers();
            server = new KayakServer
            {
                Responders = new List<IKayakResponder>
                    (
                    typeof(KayakResponder).Assembly.GetTypes()
                        .Where(t => typeof(KayakResponder).IsAssignableFrom(t) && t.IsAbstract == false)
                        
                        // to ensure that we would get consistent order, so we would always 
                        // have the responders using the same order, otherwise we get possibly
                        // random ordering, and that might cause issues
                        .OrderBy(x => x.Name)

                        .Select(t => (KayakResponder)Activator.CreateInstance(t))
                        .Select(r =>
                        {
                            r.Database = database;
                            return r as IKayakResponder;
                        })
                    )
            };
            server.Start(new IPEndPoint(IPAddress.Any, port));

        }

        public void Dispose()
        {
            server.Stop();
            database.Dispose();
        }
    }
}