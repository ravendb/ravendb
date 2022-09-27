using System;
using System.Collections.Generic;
using System.IO;
using FastTests.Corax.Bugs;
using FastTests.Voron;
using Parquet.Thrift;
using SharpCompress.Compressors;
using SharpCompress.Compressors.Deflate;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.RawData;
using Xunit;
using Xunit.Abstractions;
using Encoding = System.Text.Encoding;

namespace StressTests.Corax.Bugs;

public class CompactTreeOptimizedLookup : StorageTest
{
    private readonly ITestOutputHelper _testOutputHelper;

    public CompactTreeOptimizedLookup(ITestOutputHelper output, ITestOutputHelper testOutputHelper) : base(output)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public void CanUseTryGetNextValue()
    {
        using (var wtx = Env.WriteTransaction())
        {
            CompactTree dates = wtx.CompactTreeFor("Dates");
            foreach ( var terms in ReadTermsFromResource("Terms.log.gz"))
            {
                dates.InitializeStateForTryGetNextValue();
                for (var index = 0; index < terms.Count; index++)
                {
                    var term = terms[index];
                    byte[] key = Encoding.UTF8.GetBytes(term);
                    if (dates.TryGetNextValue(key, out var v, out var encodedKey) == false)
                    {
                        dates.Add(key, 3333333, encodedKey);
                    }
                }
            }
            
            dates.Verify();
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

        yield return  adds;
    }
}
