using System;
using System.IO;
using System.Threading.Tasks;
using FastTests.Server.Basic;
using FastTests.Server.Documents.Alerts;
using FastTests.Server.Documents.Patching;
using FastTests.Server.Documents.Replication;
using FastTests.Server.Documents.SqlReplication;
using Sparrow.Json;
using Sparrow.Logging;

namespace Tryouts
{
    public class Program
    {
        static unsafe void Main(string[] args)
        {
            var readAllBytes = File.ReadAllBytes(@"C:\Users\ayende\Downloads\docs (1)");
            using(var s = JsonOperationContext.ShortTermSingleUse())
            fixed (byte* p = readAllBytes)
            {
                var blittableJsonReaderObject = new BlittableJsonReaderObject(p, readAllBytes.Length,s);
                Console.WriteLine(blittableJsonReaderObject.ToString());
            }
        }
    }

}

