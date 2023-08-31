using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using FastTests;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19852 : RavenTestBase
{
    public RavenDB_19852(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void ResponseFromMultiGetIsCompressed()
    {
        using var store = GetDocumentStore();
        const string docs = "/docs";

        using (var session = store.OpenSession())
        {
            session.Store(new Company() {Name = new string('a', 10), Id = "doc/1"}, id: "doc/1");
            session.SaveChanges();
        }

        using (var commands = store.Commands())
        {
            using var firstCommand = new MultiGetCommandForCompressionTest(commands.RequestExecutor,
                new List<GetRequest> {new GetRequest {Url = docs, Query = "?id=doc/1"}});

            commands.RequestExecutor.Execute(firstCommand, commands.Context);
        }
    }

    private class MultiGetCommandForCompressionTest : MultiGetCommand
    {
        public MultiGetCommandForCompressionTest(RequestExecutor requestExecutor, List<GetRequest> commands) : base(requestExecutor, commands)
        {
            ResponseType = RavenCommandResponseType.Raw;
        }

        internal MultiGetCommandForCompressionTest(RequestExecutor requestExecutor, List<GetRequest> commands, SessionInfo sessionInfo) : base(requestExecutor, commands,
            sessionInfo)
        {
        }

        public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
        {
            // Automatic decompression clears info about content-encodings. We've to assert by ContentType.
            Assert.Contains("GZip", response.Content.GetType().Name);
        }
    }
}
