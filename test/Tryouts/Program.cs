using System;
using System.Threading.Tasks;
using FastTests.Voron;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
using Xunit;
using Sparrow.Platform;
using System.Linq;
using FastTests.Blittable;
using FastTests.Issues;
using FastTests.Voron.RawData;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var a = new FastTests.Server.Replication.ReplicationConflictsTests())
            {
                a.Conflict_then_patch_request_will_return_409_and_conflict_data();
            }
        }
    }
}

