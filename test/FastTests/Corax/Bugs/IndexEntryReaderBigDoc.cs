using System.IO;
using Corax;
using Corax.Mappings;
using Newtonsoft.Json.Linq;
using Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;
using Sparrow.Server;
using Sparrow.Threading;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class IndexEntryReaderBigDoc : NoDisposalNeeded
{
    private readonly ITestOutputHelper _testOutputHelper;

    public IndexEntryReaderBigDoc(ITestOutputHelper output, ITestOutputHelper testOutputHelper) : base(output)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public unsafe void CanCreateAndReadBigDocument()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var scope = new SingleEntryWriterScope(allocator);
        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, "id()")
            .AddBinding(1, "Badges");
        using var knownFields = builder.Build();
        //var indexEntryWriter = new IndexEntryWriter(allocator, knownFields);

        Assert.Fail("implement me");
        //var enumerableWriterScope = new EnumerableWriterScope(new(), new(), new(), new(), new(), allocator);

        // for (int i = 0; i < 7500; i++)
        // {
        //     enumerableWriterScope.Write(string.Empty, 1, "Nice Answer", ref indexEntryWriter);
        // }
        //
        // enumerableWriterScope.Finish(string.Empty, 1, ref indexEntryWriter);

        //scope.Write(string.Empty, 0, "users/1", ref indexEntryWriter);

        // indexEntryWriter.Finish(out var output);
        //
        // new IndexEntryReader(output.Ptr, output.Length).GetFieldReaderFor(0).Read(out var id);
    }

    private static JArray ReadDocFromResource(string file)
    {
        var reader = new StreamReader(typeof(PostingListAddRemoval).Assembly.GetManifestResourceStream("FastTests.Corax.Bugs." + file));
        return JArray.Parse(reader.ReadToEnd());
    }
}
