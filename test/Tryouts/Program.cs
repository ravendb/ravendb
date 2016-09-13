using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Indexes;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var store = new DocumentStore
            {
                Url = "http://localhost.fiddler:8080"
            })
            {
                store.Initialize();
                for (int i = 0; i < 300; i++)
                {
                    try
                    {
                        store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                        {
                            Id = "db" + i,
                            Settings =
                            {
                                ["Raven/DataDir"] = "~/Databases/db" + i
                            }
                        });
                    }
                    catch (Exception)
                    {
                        Console.WriteLine("db" + i + " already exists");
                    }

                    switch (i % 3)
                    {
                        case 0:
                            break;
                        case 1:
                            for (int j = 0; j < 20; j++)
                            {
                                store.DatabaseCommands.ForDatabase("db" + i).PutIndex("index_" + j,
                                    new Raven.Client.Indexing.IndexDefinition
                                    {
                                        Maps =
                                        {
                                            "from u in docs.Users select new { u.Name}"
                                        }
                                    });
                            }
                            break;
                        case 2:
                            for (int j = 0; j < 10; j++)
                            {
                                store.DatabaseCommands.ForDatabase("db"+i).PutIndex("index_" + j,
                                    new Raven.Client.Indexing.IndexDefinition
                                    {
                                        Maps =
                                        {
                                            "from u in docs.Users select new { u.Name}"
                                        }
                                    });
                            }
                            for (int j = 0; j < 10; j++)
                            {
                                store.DatabaseCommands.ForDatabase("db" + i).PutIndex("second_index_" + j,
                                    new Raven.Client.Indexing.IndexDefinition
                                    {
                                        Maps =
                                        {
                                            "from u in docs.Products select new { u.Name}"
                                        }
                                    });
                            }
                            break;
                    }
                }
            }
        }

    }
}

