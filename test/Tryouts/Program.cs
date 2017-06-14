using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;

namespace Tryouts
{
    public class Program
    {
        public class User
        {
            public string Name;
        }

        public class ByName : AbstractIndexCreationTask<User, ByName.Result>
        {

            public class Result
            {
                public string Name { get; set; }
                public int Count { get; set; }
            }

            public ByName()
            {
                Map = users => from u in users
                               select new
                               {
                                   Count = 1,
                                   Name = u.Name
                               };

                Reduce = results => from r in results
                                    group r by r.Name into g
                                    select new
                                    {
                                        Name = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }

        public static void Main(string[] args)
        {

            using (var store = new DocumentStore
            {
                Urls = new[]
                {
                    "http://localhost:8080"
                },
                Database = "db1"
            })
            {
                store.Initialize();
                new ByName().Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 250_000; i++)
                    {
                        bulk.Store(new User
                        {
                            Name = "marcin"
                        });
                    }
                }
            }
        }
    }
}
