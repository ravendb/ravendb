using System;
using System.Collections.Generic;
using System.IO;
using FastTests.Voron;
using Parquet.Thrift;
using SharpCompress.Compressors;
using SharpCompress.Compressors.Deflate;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.RawData;
using Voron.Data.Sets;
using Xunit;
using Xunit.Abstractions;
using Encoding = System.Text.Encoding;

namespace FastTests.Corax.Bugs;

public class CompactTreeAddAndRemove : StorageTest
{
    private readonly ITestOutputHelper _testOutputHelper;

    public CompactTreeAddAndRemove(ITestOutputHelper output, ITestOutputHelper testOutputHelper) : base(output)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public unsafe void AddAndRemoveValues()
    {
        using (var wtx = Env.WriteTransaction())
        {
            CompactTree dates = wtx.CompactTreeFor("Dates");
            foreach ( var terms in ReadTermsFromResource("repro-2.log.gz"))
            {
                for (var index = 0; index < terms.Count; index++)
                {
                    var term = terms[index];
                    var parts = term.Split(' ');
                    var key = Encoding.UTF8.GetBytes(parts[1]);
                    switch (parts[0])
                    {
                        case "+":
                            dates.Add(key, long.Parse(parts[2]));
                            break;
                        case "-":
                            dates.TryRemove(key, out var old);
                            Assert.Equal(long.Parse(parts[2]), old);
                            break;
                    }
                }
            }
            
            dates.Verify();
            dates.VerifyOrderOfElements();
            foreach (long page in dates.AllPages())
            {
                var state = new CompactTree.CursorState { Page = wtx.LowLevelTransaction.GetPage(page), };
                Assert.Equal(state.ComputeFreeSpace(), state.Header->FreeSpace);
            }
        }
    }
    
    private static IEnumerable<List<string>> ReadTermsFromResource(string file)
    {
        var reader = new StreamReader(new GZipStream(typeof(SetAddRemoval).Assembly.GetManifestResourceStream("FastTests.Corax.Bugs." + file), CompressionMode.Decompress));
        var adds = new List<string>();
        string line = null;
        while ((line = reader.ReadLine()) != null)
        {
            if (line.StartsWith("#"))
            {
                yield return adds;
                adds.Clear();
                continue;
            }

            adds.Add(line);
        }

        yield return  adds;
    }
}
