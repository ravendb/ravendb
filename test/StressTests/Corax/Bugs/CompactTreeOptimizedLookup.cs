using System.Collections.Generic;
using System.IO;
using FastTests.Voron;
using SharpCompress.Compressors;
using SharpCompress.Compressors.Deflate;
using Voron.Data.CompactTrees;
using Xunit;
using Xunit.Abstractions;
using Encoding = System.Text.Encoding;

namespace StressTests.Corax.Bugs;

public class CompactTreeOptimizedLookup : StorageTest
{
    public CompactTreeOptimizedLookup(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanUseTryGetNextValue()
    {
        using (var wtx = Env.WriteTransaction())
        {
            CompactTree dates = wtx.CompactTreeFor("Dates");
            var ck = new CompactKey();
            ck.Initialize(wtx.LowLevelTransaction);
            foreach (var terms in ReadTermsFromResource("Terms.log.gz"))
            {
                dates.InitializeStateForTryGetNextValue();
                for (var index = 0; index < terms.Count; index++)
                {
                    var term = terms[index];
                    byte[] key = Encoding.UTF8.GetBytes(term);
                    ck.Set(key);
                    if (dates.TryGetNextValue(ck, out _, out var v, out _) == false)
                    {
                        dates.Add(ck, 3333333);
                    }
                }
            }

            dates.VerifyOrderOfElements();
        }
    }

    private static IEnumerable<List<string>> ReadTermsFromResource(string file)
    {
        var reader = new StreamReader(new GZipStream(typeof(CompactTreeOptimizedLookup).Assembly.GetManifestResourceStream("StressTests.Corax.Bugs." + file), CompressionMode.Decompress));
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
