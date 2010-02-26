using System;
using System.Collections.Generic;
using Kayak;
using Rhino.DivanDB.Server.Responders;
using System.Linq;

namespace Rhino.DivanDB.Server
{
    class Program
    {
        static void Main()
        {
            using (var db = new DocumentDatabase("Db"))
            {
                db.SpinBackgroundWorkers();

                var server = new KayakServer
                {
                    Responders = new List<IKayakResponder>
                    (
                        typeof(KayakResponder).Assembly.GetTypes()
                            .Where(t=>typeof(KayakResponder).IsAssignableFrom(t) && t.IsAbstract == false)
                            .Select(t => (KayakResponder)Activator.CreateInstance(t))
                            .Select(r =>
                            {
                                r.Database = db;
                                return r as IKayakResponder;
                            })
                    )
                };
                server.Start();

                Console.WriteLine("Ready to process requests...");
                Console.ReadLine();

                server.Stop();
            }
        }
    }
}
