using System;
using Raven.Client.Document;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var a = new DocumentStore
            {
                DefaultDatabase = "test",
                Url = "http://live-test.ravendb.net"
            }.Initialize();

            using (var s = a.OpenSession())
            {
                s.Store(new
                {
                    Worked = true,
                    Awesome = 100
                });
                s.SaveChanges();
            }

        }
    }
}
