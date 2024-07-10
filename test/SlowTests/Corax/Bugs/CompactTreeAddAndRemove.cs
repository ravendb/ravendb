using System.Collections.Generic;
using System.IO;
using FastTests.Voron;
using SharpCompress.Compressors;
using SharpCompress.Compressors.Deflate;
using Tests.Infrastructure;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Xunit;
using Xunit.Abstractions;
using Encoding = System.Text.Encoding;

namespace SlowTests.Corax.Bugs;

public class CompactTreeAddAndRemove : StorageTest
{
    public CompactTreeAddAndRemove(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData("repro-2.log.gz")]
    [InlineData("repro-4.log.gz")]
    public unsafe void AddAndRemoveValues(string filename)
    {
        using (var wtx = Env.WriteTransaction())
        {
            int i = 0;

            CompactTree dates = wtx.CompactTreeFor("Dates");
            foreach (var terms in ReadTermsFromResource(filename))
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

                    i++;
                }
            }

            dates.VerifyOrderOfElements();
            foreach (long page in dates.AllPages())
            {
                var state = new Lookup<CompactTree.CompactKeyLookup>.CursorState() { Page = wtx.LowLevelTransaction.GetPage(page), };
                Assert.Equal(state.ComputeFreeSpace(), state.Header->FreeSpace);
            }
        }
    }

    private static IEnumerable<List<string>> ReadTermsFromResource(string file)
    {
        var reader = new StreamReader(new GZipStream(typeof(CompactTreeAddAndRemove).Assembly.GetManifestResourceStream("SlowTests.Corax.Bugs." + file), CompressionMode.Decompress));
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

        yield return adds;
    }
}
