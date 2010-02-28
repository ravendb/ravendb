using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Newtonsoft.Json.Linq;
using System.Linq;
using Rhino.DivanDB.Json;
using Rhino.DivanDB.Linq;

namespace Rhino.DivanDB.Tryouts
{
    class Program
    {
        const string query = @"
    from doc in docs
    where doc.type <= 1 && doc.user.is_active == false
    select new { Key = doc.title, Value = doc.content, Size = doc.size };
";
        public static void Main()
        {
            try
            {
                var linqTransformer = new LinqTransformer("pagesByTitle", query, "docs", Path.GetTempPath(), typeof(JsonDynamicObject));
                linqTransformer.Compile();
                File.WriteAllText("a.txt",linqTransformer.LinqQueryToImplicitClass());
                Process.Start("a.txt");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

//            using (var db = new DocumentDatabase("Db"))
//            {
//                db.SpinBackgroundWorkers();

//                db.PutIndex("pagesByTitle", @"
//    from doc in docs
//    where doc.type == ""page""
//    select new {doc.title}");

//                db.Put(JObject.Parse("{type: 'page', title: 'what will happen? michael' }"));
//                db.Put(JObject.Parse("{type: 'page', title: 'ayende is in the pub' }"));

//                for (int i = 0; i < 5; i++)
//                {
//                    var sw = Stopwatch.StartNew();
//                    QueryResult query;
//                    do
//                    {
//                        query = db.Query("pagesByTitle", "title:michael");
//                    } while (query.IsStale);
//                    Console.WriteLine(sw.ElapsedMilliseconds);
//                    foreach (var result in query.Results)
//                    {
//                        Console.WriteLine(result);
//                    }

//                }
//            }
        }
    }
}