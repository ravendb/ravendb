using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.Containers;
using Xunit;

namespace FastTests.Corax.Bugs
{
    public class RavenDB_21223 : StorageTest
    {
        public RavenDB_21223(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ContainerAllocateDeleteAndAllocate()
        {
            List<long> toDelete = new();

            {
                using var wtx = Env.WriteTransaction();
                var rootContainer = wtx.OpenContainer("TestContainer");
                for (int i = 0; i < 1000; i++)
                {
                    var allocatedPage = (long)Container.Allocate(wtx.LowLevelTransaction, rootContainer, 50, out var space);
                    space.Fill(1);
                }

                wtx.Commit();
            }


            {
                using var wtx = Env.WriteTransaction();
                var rootContainer = wtx.OpenContainer("TestContainer");
                for (int i = 0; i < 1000; i++)
                {
                    var allocatedPage = (long)Container.Allocate(wtx.LowLevelTransaction, rootContainer, 50, out var space);
                    space.Fill(1);
                    toDelete.Add(allocatedPage);
                }
                foreach (var id in toDelete)
                    Container.Delete(wtx.LowLevelTransaction, rootContainer, new ContainerEntryId(id));

                var newPage = (long)Container.Allocate(wtx.LowLevelTransaction, rootContainer, 50, out var allocatedSpace);
                allocatedSpace.Fill(2);

                wtx.Commit();
            }
        }
    }
}
