using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Corax;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            //XunitLogging.RedirectStreams = false;
        }

        public static async Task Main(string[] args)
        {
            if (Directory.Exists("test"))
                Directory.Delete("test", true);
            using var options = StorageEnvironmentOptions.ForPath("test");
            using var env = new StorageEnvironment(options);

            using (var indexWriter = new IndexWriter(env))
            {
                using var ctx = JsonOperationContext.ShortTermSingleUse();

                var entry = ctx.ReadObject(
                    new DynamicJsonValue {["Name"] = "Oren", ["Gender"] = "Male", ["Dogs"] = new DynamicJsonArray {"Pheobe", "Arava", "Oscar"}},
                    "indexentry");

                indexWriter.Index("users/1", entry);

            
                var entry2 = ctx.ReadObject(
                    new DynamicJsonValue {["Name"] = "Ayende", ["Gender"] = "Male", ["Dogs"] = new DynamicJsonArray {"Arava"}},
                    "indexentry");

                indexWriter.Index("users/2", entry2);

                var entry3 = ctx.ReadObject(
                    new DynamicJsonValue {["Name"] = "Rachel", ["Gender"] = "Female", ["Dogs"] = new DynamicJsonArray {"Arava"}},
                    "indexentry");

                indexWriter.Index("users/3", entry3);

                
                indexWriter.Commit();
            }

            using (var searcher = new IndexSearcher(env))
            {
                QueryOp q = new BinaryQuery(
                    new QueryOp[] {new TermQuery("Dogs", "Arava"), new TermQuery("Gender", "Male"),},
                    BitmapOp.And
                );
                var results = searcher.Query(q);

                foreach (object result in results)
                {
                    Console.WriteLine(result);
                }


            }

            
            Console.WriteLine("Done");
        }
    }
}
