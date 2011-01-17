using Raven.Client.Document;

namespace Raven.Tryouts
{
    class Program
    {
        static void Main()
        {
			DocumentStore store = new DocumentStore { Url = "http://localhost:8080" };
			store.Initialize();

			using (var session = store.OpenSession())
			{
				session.Store(new{Name = "Ayende"});
				session.SaveChanges();

			}

        }
    }
}
