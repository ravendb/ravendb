using System;
using Newtonsoft.Json.Linq;

namespace Rhino.DivanDB.Tryouts
{
    class Program
    {
        public static void Main()
        {
            var db = new DocumentDatabase("Db");
            for (int i = 0; i < 5; i++)
            {
                string documentId = db.Put(JObject.Parse("{first_name: 'ayende', last_name: 'rahien'}"));
                Console.WriteLine(documentId);
                var val = db.Get(documentId);
                Console.WriteLine(val);
            }
            db.AddView(@"var pagesByTitle = 
    from doc in docs
    select new { name = doc.first_name };
");
        }
    }
}
