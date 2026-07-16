using System;
using System.Buffers.Binary;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.PowerBI;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Integrations.PostgreSQL;

public class RavenDB_26998 : RavenTestBase
{
    public RavenDB_26998(ITestOutputHelper output) : base(output)
    {
    }

    private class Company
    {
        public string Name { get; set; }
        public string Phone { get; set; }
    }

    private class Companies_ByName : AbstractIndexCreationTask<Company>
    {
        public Companies_ByName()
        {
            Map = companies => from c in companies select new { c.Name };
        }
    }
    
    private const int PreviewOuterCap = 1000001;

    [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
    public async Task Preview_HonorsInnerLimit_LikeLoad()
    {
        using var store = GetDocumentStore();
        await SeedCompanies(store, count: 20);
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        const string rawLoadSql = """
            SELECT *
            FROM "public"."Companies"
            LIMIT 5
            """;

        var wrappedPreviewSql = $"""
            select * from
            (
                {rawLoadSql}
            ) "_"
            limit {PreviewOuterCap}
            """;

        var loadRows = await CountRows(rawLoadSql, database);
        var previewRows = await CountRows(wrappedPreviewSql, database);

        Assert.Equal(5, loadRows);
        Assert.Equal(loadRows, previewRows);
    }

    [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
    public async Task Preview_HonorsInnerLimitAndOffset_LikeLoad()
    {
        using var store = GetDocumentStore();
        await SeedCompanies(store, count: 20);
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        const string rawLoadSql = """
            SELECT *
            FROM "public"."Companies"
            WHERE "Name" <> 'Company-19'
            ORDER BY "Name"
            LIMIT 5 OFFSET 10
            """;

        var wrappedPreviewSql = $"""
            select * from
            (
                {rawLoadSql}
            ) "_"
            limit {PreviewOuterCap}
            """;

        var loadRows = await CountRows(rawLoadSql, database);
        var previewRows = await CountRows(wrappedPreviewSql, database);

        Assert.Equal(5, loadRows);
        Assert.Equal(loadRows, previewRows);
    }

    [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
    public async Task Preview_HonorsInnerLimit_OnIndex()
    {
        using var store = GetDocumentStore();
        await SeedCompanies(store, count: 20);
        await new Companies_ByName().ExecuteAsync(store);
        Indexes.WaitForIndexing(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        
        const string wrappedPreviewSql = $"""
            select * from
            (
                SELECT *
                FROM "indexes"."Companies/ByName"
                LIMIT 5
            ) "_"
            limit 1000001
            """;

        var previewRows = await CountRows(wrappedPreviewSql, database);

        Assert.Equal(5, previewRows);
    }

    private static async Task SeedCompanies(Raven.Client.Documents.IDocumentStore store, int count)
    {
        using var session = store.OpenAsyncSession();
        for (int i = 0; i < count; i++)
            await session.StoreAsync(new Company { Name = $"Company-{i:D2}", Phone = i.ToString() });
        await session.SaveChangesAsync();
    }
    
    private static async Task<int> CountRows(string sql, Raven.Server.Documents.DocumentDatabase database)
    {
        Assert.True(PowerBIFetchQuery.TryParse(sql, Array.Empty<int>(), database, out var query));
        using (query)
        {
            var token = TestContext.Current.CancellationToken;
            await query.Init();

            var pipe = new Pipe();
            var builder = new MessageBuilder();

            var readTask = ReadAllAsync(pipe.Reader, token);
            await query.Execute(builder, pipe.Writer, token);
            await pipe.Writer.CompleteAsync();
            var bytes = await readTask;

            return ParseCommandCompleteRowCount(bytes);
        }
    }

    private static async Task<byte[]> ReadAllAsync(PipeReader reader, CancellationToken token)
    {
        var ms = new MemoryStream();
        while (true)
        {
            var result = await reader.ReadAsync(token);
            foreach (var segment in result.Buffer)
                ms.Write(segment.Span);
            reader.AdvanceTo(result.Buffer.End);
            if (result.IsCompleted)
                break;
        }
        await reader.CompleteAsync();
        return ms.ToArray();
    }
    
    private static int ParseCommandCompleteRowCount(byte[] buffer)
    {
        int i = 0;
        while (i + 5 <= buffer.Length)
        {
            var type = buffer[i];
            int length = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(i + 1, 4));
            int payloadStart = i + 5;
            int payloadLength = length - 4;
            if (payloadLength < 0 || payloadStart + payloadLength > buffer.Length)
                break;

            if (type == (byte)'C')
            {
                var tag = Encoding.ASCII.GetString(buffer, payloadStart, payloadLength).TrimEnd('\0');
                var parts = tag.Split(' ');
                if (parts.Length == 2 && parts[0] == "SELECT" && int.TryParse(parts[1], out var n))
                    return n;
            }

            i = payloadStart + payloadLength;
        }

        throw new InvalidOperationException("No CommandComplete 'SELECT N' tag found in Execute output.");
    }
}
