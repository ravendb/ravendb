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
            
        }
    }
}