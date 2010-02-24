using System;
using Newtonsoft.Json.Linq;

namespace Rhino.DivanDB.Tryouts
{
    class Program
    {
        public static void Main()
        {
            var db = new DocumentDatabase("Db");

            var query = db.Query("pagesByTitle", "name:ayende");
            foreach (var q in query)
            {
                Console.WriteLine(q);
            }
        }
    }
}
