using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests.Voron;
using Voron.Data.Lookups;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class RavenDB_21321 : StorageTest
{
    public RavenDB_21321(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanBulkInsertToLookup()
    {
        long k = 0;
        using (var wtx = Env.WriteTransaction())
        {
            var lookup = wtx.LookupFor<Int64LookupKey>("test");
            var changes = lookup.CheckTreeStructureChanges();
            while (changes.Changed == false)
            {
                lookup.Add(++k, 0);
            }

            wtx.Commit();
        }
        
        using (var wtx = Env.WriteTransaction())
        {
            var lookup = wtx.LookupFor<Int64LookupKey>("test");
            var changes = lookup.CheckTreeStructureChanges();
            while (changes.Changed == false)
            {
                lookup.TryRemove(k--);
            }

            wtx.Commit();
        }

    }
}

