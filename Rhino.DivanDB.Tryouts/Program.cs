using System;
using Newtonsoft.Json.Linq;

namespace Rhino.DivanDB.Tryouts
{
    class Program
    {
        public static void Main()
        {
            var db = new DocumentDatabase("Db");
            db.SpinBackgroundWorkers();

            db.AddView(@"var pagesByTitle = 
from doc in docs
where doc.type == ""page""
select new {doc.title}"
                );

            db.Put(JObject.Parse("{type: 'page', title: 'ayende in the pub'}"));
            db.Put(JObject.Parse("{type: 'page', title: 'what will happen?'}"));

            Console.ReadLine();
            var query = db.Query("pagesByTitle", "+title:ayende");
            foreach (var q in query)
            {
                Console.WriteLine(q);
            }   
            db.Dispose();
        }
    }
}
