using System;
using System.Collections.Generic;
using System.Diagnostics;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace Rhino.DivanDB.Tryouts
{
    class Program
    {
        public static void Main()
        {
            using (var db = new DocumentDatabase("Db"))
            {
                db.SpinBackgroundWorkers();

                db.PutView(@"var pagesByTitle = 
from doc in docs
where doc.type == ""page""
select new {doc.title}");

                db.Put(JObject.Parse("{type: 'page', title: 'what will happen?' }"));
                db.Put(JObject.Parse("{type: 'page', title: 'ayende is in the pub' }"));

                QueryResult query;
                do
                {
                    query = db.Query("pagesByTitle", "title:ayende");
                } while (query.IsStale);

                foreach (var result in query.Results)
                {
                    Console.WriteLine(result);
                }

            }
        }
    }
}