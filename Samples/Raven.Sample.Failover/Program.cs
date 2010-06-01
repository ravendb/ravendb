using System;
using Raven.Client.Document;

namespace Raven.Sample.Failover
{
    class Program
    {
        static void Main(string[] args)
        {
			var documentStore1 = new DocumentStore { Url = "http://localhost:8080" }.Initialize();

            using (var session1 = documentStore1.OpenSession())
            {
                session1.Store(new User { Id = "users/ayende", Name = "Ayende" });
                session1.SaveChanges();
            }

            using (var session1 = documentStore1.OpenSession())
            {
                Console.WriteLine(session1.Load<User>("users/ayende").Name);
            }

            Console.WriteLine("Wrote one docuemnt to 8080, ready for server failure");

            Console.ReadLine();

            using (var session1 = documentStore1.OpenSession())
            {
                Console.WriteLine(session1.Load<User>("users/ayende").Name);
            }

        }
    }
}
