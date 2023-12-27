using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using FastTests;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19852 : RavenTestBase
{
    public RavenDB_19852(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData(null)]
    [InlineData(HttpCompressionAlgorithm.Gzip)]
#if FEATURE_BROTLI_SUPPORT
    [InlineData(HttpCompressionAlgorithm.Brotli)]
#endif
    [InlineData(HttpCompressionAlgorithm.Zstd)]
    public void ResponseFromMultiGetIsCompressed(HttpCompressionAlgorithm? compressionAlgorithm)
    {
        using var store = GetDocumentStore(new Options
        {
            ModifyDocumentStore = s =>
            {
                if (compressionAlgorithm == null)
                    return;

                s.Conventions.HttpCompressionAlgorithm = compressionAlgorithm.Value;
            }
        });

        const string docs = "/docs";

        using (var session = store.OpenSession())
        {
            session.Store(new Company() { Name = new string('a', 10), Id = "doc/1" }, id: "doc/1");
            session.SaveChanges();
        }

        using (var commands = store.Commands())
        {
            using var firstCommand = new MultiGetCommandForCompressionTest(
                compressionAlgorithm ?? DocumentConventions.DefaultHttpCompressionAlgorithm,
                commands.RequestExecutor,
                new List<GetRequest> { new GetRequest { Url = docs, Query = "?id=doc/1" } });

            commands.RequestExecutor.Execute(firstCommand, commands.Context);
        }
    }

    private class MultiGetCommandForCompressionTest : MultiGetCommand
    {
        private readonly HttpCompressionAlgorithm _compressionAlgorithm;

        public MultiGetCommandForCompressionTest(HttpCompressionAlgorithm compressionAlgorithm, RequestExecutor requestExecutor, List<GetRequest> commands) : base(requestExecutor, commands)
        {
            _compressionAlgorithm = compressionAlgorithm;
            ResponseType = RavenCommandResponseType.Raw;
        }

        public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
        {
            var streamWithTimeout = (StreamWithTimeout)stream;
            var innerStream = streamWithTimeout._stream;
            var contentTypeName = innerStream.GetType().Name;

            switch (_compressionAlgorithm)
            {
                case HttpCompressionAlgorithm.Gzip:
                    Assert.True(contentTypeName.Contains("GZip"), $"{contentTypeName}.Contains('GZip')");
                    break;
#if FEATURE_BROTLI_SUPPORT
                case HttpCompressionAlgorithm.Brotli:
                    Assert.True(contentTypeName.Contains("Brotli"), $"{contentTypeName}.Contains('Brotli')");
                    break;
#endif
                case HttpCompressionAlgorithm.Zstd:
                    Assert.True(contentTypeName.Contains("Zstd"), $"{contentTypeName}.Contains('Zstd')");
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}
