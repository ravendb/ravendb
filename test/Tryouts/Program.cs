using System;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Abstractions.Extensions;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Linq;

namespace Tryouts
{
    public class Program
    {
        public static int Numofdocs = 500000;
        public class User
        {
            public string FirstName { get; set; }

            public string LastName { get; set; }

            public string[] Tags { get; set; }
            public string LongAttributte { get; set; }
        }

        public static int Id = 1;

        public static void Main(string[] args)
        {
            if (args.Length == 1)
                Numofdocs = Convert.ToInt32(args[0]);
            while (true)
            { 
                try
                {
                    for (int i = 0; i < 10; i++)
                    {
                        using (var store = new DocumentStore
                        {
                            Url = "http://127.0.0.1:8080",
                            DefaultDatabase = "test"
                        })
                        {
                            store.Initialize();

                            try
                            {
                                createDb(store).Wait();
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("createDb returned exception");
                            }
                            BulkInsert(store, Numofdocs).Wait();
                            Console.WriteLine("Done insertion");
                            //  Console.ReadKey();
                        }
                    }
                    using (var store = new DocumentStore
                    {
                        Url = "http://127.0.0.1:8080",
                        DefaultDatabase = "test"
                    })
                    {
                        store.Initialize();
                        using (var session = store.OpenSession())
                        {
                            var users = session.Query<User>().Take(10).ToList();
                            int i = 0;
                            users.ForEach(x =>
                            {
                                if (!i.ToString().Equals(x.FirstName))
                                    Console.WriteLine("Error at user " + i);
                                ++i;
                            });

                        }
                        Console.WriteLine("Done checking");
                        Console.ReadKey();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    Console.ReadKey();
                }
            }
        }

        public static async Task createDb(DocumentStore store)
        {
            var doc = MultiDatabase.CreateDatabaseDocument("test");
            await store.AsyncDatabaseCommands.GlobalAdmin.CreateDatabaseAsync(doc).ConfigureAwait(false);
        }

        public static async Task BulkInsert(DocumentStore store, int numOfItems)
        {
            Console.WriteLine("Doing bulk-insert...");

            string[] tags = null;// Enumerable.Range(0, 1024*8).Select(x => "Tags i" + x).ToArray();

            var sp = System.Diagnostics.Stopwatch.StartNew();
            using (var bulkInsert = store.BulkInsert())
            {
                // int id = 1;
                for (int i = 0; i < numOfItems; i++)
                {
                    if (i % 100000 == 0)
                        Console.WriteLine(i.ToString("#,#"));
                    var entity = new User
                    {
                        FirstName = $"{i}",
                        LastName = $"Last Name - {i}",
                        LongAttributte = $@"
fskladf f;alsdl;f ;alksdjf 'pasdjf l;kasdfmk;laerj gklfmdklgkn fcnfdklcvn ,mnb.,xmcv lskdf nv;okljdfng v/dvm z/s;xvmz/dx.,jvm /.zxm /z.x, c'
sdklfn sdlkgm vlsdf,bm kjsfq[wptuog934t345t90345 430wtioe frsd'fpo ;lvkmcm z.x, fjmnv l.zx,dmv zsdkfv mn;QOI LFC,M.XJFCKLS.EHFKZ,DGJWY IGFq3eq
DAASF CM,XIhn f,mvLJ oiu9 IOPop oiJOIPjiopPOJPOJ POJ LK",
                        Tags = tags
                    };
                    await bulkInsert.StoreAsync(entity, $"users/{Id++}");
                }
            }
            Console.WriteLine("\r\ndone in " + sp.Elapsed + " rate of " + (Math.Round(numOfItems / sp.Elapsed.TotalSeconds, 2).ToString("#,#.##")) + " docs / sec");
        }
    }
}
