using System;
using System.Collections.Generic;
using System.IO;
using Corax;
using FastTests.Voron;
using Newtonsoft.Json.Linq;
using Parquet.Thrift;
using Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;
using SharpCompress.Compressors;
using SharpCompress.Compressors.Deflate;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.RawData;
using Xunit;
using Xunit.Abstractions;
using Encoding = System.Text.Encoding;

namespace FastTests.Corax.Bugs;

public class IndexEntryReaderBigDoc : NoDisposalNeeded
{
    private readonly ITestOutputHelper _testOutputHelper;

    public IndexEntryReaderBigDoc(ITestOutputHelper output, ITestOutputHelper testOutputHelper) : base(output)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public void CanCreateAndReadBigDocument()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var scope = new SingleEntryWriterScope(allocator);
        var knownFields = new IndexFieldsMapping(allocator)
            .AddBinding(0, "id()")
            .AddBinding(1, "Badges");
        var indexEntryWriter = new IndexEntryWriter(allocator, knownFields);

        var enumerableWriterScope = new EnumerableWriterScope(new(), new(), new(), new(), new(), allocator);

        for (int i = 0; i < 7500; i++)
        {
            enumerableWriterScope.Write(string.Empty, 1, "Nice Answer", ref indexEntryWriter);
        }
      
        enumerableWriterScope.Finish(string.Empty, 1, ref indexEntryWriter);
        
        scope.Write(string.Empty, 0, "users/1", ref indexEntryWriter);

        indexEntryWriter.Finish(out var output);

        new IndexEntryReader(output.ToSpan()).GetReaderFor(0).Read(out var id);
    }
    
    private static JArray  ReadDocFromResource(string file)
    {
        var reader = new StreamReader(typeof(SetAddRemoval).Assembly.GetManifestResourceStream("FastTests.Corax.Bugs." + file));
        return JArray.Parse(reader.ReadToEnd());
    }
}
