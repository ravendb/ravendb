using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents;
using SlowTests.Server.Documents.AI.GenAi;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Sdk;

namespace StressTests.Server.Documents.AI.GenAi;

public class RavenDB_24884 : RavenTestBase
{
    public RavenDB_24884(ITestOutputHelper output) : base(output)
    {
    }

    private static readonly string BananaPngBase64 = GetFileAsBase64("banana.png");

    [RavenTheory(RavenTestCategory.Ai | RavenTestCategory.Etl)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Skip = "Takes too long")]
    public async Task TextFileWithTooManyTokens(Options options, GenAiConfiguration config)
    {
        var titles = "Date,Author,Category,Fact";
        string[] rows = new[]
        {
            "2000-01-01,Anders Hejlsberg,CSharp,C# was designed at Microsoft as part of the .NET initiative",
            "2002-10-01,ECMA,CSharp,C# was standardized as ECMA-334 in 2002",
            "2005-11-07,Microsoft,CSharp,C# 2.0 introduced generics, partial classes, and nullable types",
            "2007-11-19,Anders Hejlsberg,CSharp,C# 3.0 added LINQ and lambda expressions",
            "2010-04-12,Mads Torgersen,CSharp,C# 4.0 added dynamic and named/optional arguments",
            "2012-08-15,Anders Hejlsberg,CSharp,C# 5.0 introduced async and await",
            "2015-07-20,Microsoft,CSharp,C# 6.0 added string interpolation and expression bodied members",
            "2017-03-07,Mads Torgersen,CSharp,C# 7.0 introduced tuples, out variables, and pattern matching",
            "2019-09-23,Microsoft,CSharp,C# 8.0 added nullable reference types and default interface methods",
            "2020-11-10,Mads Torgersen,CSharp,C# 9.0 introduced records and init only setters",
            "2021-11-08,Microsoft,CSharp,C# 10 added global using directives and file scoped namespaces",
            "2022-11-08,Mads Torgersen,CSharp,C# 11 introduced required members and raw string literals",
            "2023-11-14,Microsoft,CSharp,C# 12 added primary constructors for classes",
            "1994-10-21,Erich Gamma,DesignPatterns,Published Design Patterns book (Gang of Four)",
            "1994-10-21,Richard Helm,DesignPatterns,Co author of Design Patterns introducing 23 patterns",
            "1994-10-21,Ralph Johnson,DesignPatterns,Helped define Singleton, Factory, Observer",
            "1994-10-21,John Vlissides,DesignPatterns,Popularized patterns in OOP with the GoF",
            "2003-05-01,Martin Fowler,DesignPatterns,Published Patterns of Enterprise Application Architecture",
            "2004-05-01,Martin Fowler,DesignPatterns,Defined Dependency Injection as a pattern",
            "2006-01-01,Eric Evans,DesignPatterns,Published Domain Driven Design introducing Aggregates and Repositories",
            "1999-08-01,Robert C. Martin,DesignPatterns,Published articles that defined SOLID principles",
        };

        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Categorize the expenses in the associated file";
        config.Collection = "Transactions";
        config.SampleObject = JsonConvert.SerializeObject(new
        {
            Summary = new[]
            {
                new {
                    Category = "Expense category (food | entertainment | utilities | education)",
                    TotalSpent = 10m,
                    TransctionCount = 5,
                    Notes = "General observations on this expense category based on the actual expenses (spend too much on takeout or fees are high on utility, etc)"
                }
            }
        });
        config.UpdateScript = @"this.Summary = $output.Summary;";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
ai.genContext({
    Date: this.Date,
    Location: this.Location,
})
    .withText(loadAttachment('transactions.csv'));
"
        };

        await store.Maintenance.SendAsync(new AddGenAiOperation(config));
        // var etl = Etl.WaitForEtlToComplete(store);

        var marker = store.Database + Guid.NewGuid();
        var markerSummary = new Summary[] { new Summary(string.Empty, 0, 0, marker) };

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Transaction("users/1", new DateTime(2025, 1, 1), "New York", markerSummary), "txs/2025-01-01");
            await session.StoreAsync(new Transaction("users/2", new DateTime(2025, 1, 2), "Netanya", markerSummary), "txs/2025-01-02");

            session.Advanced.Attachments.Store("txs/2025-01-01", "transactions.csv", BuildCsvStream(titles, rows, targetMb: 7));

            session.Advanced.Attachments.Store("txs/2025-01-02", "transactions.csv", BuildCsvStream(titles, rows, targetMb: 0.01));
            await session.SaveChangesAsync();
        }

        var value = await WaitForValueAsync(async () =>
        {
            var errors = await Etl.GetItemLoadErrorsAsync(store.Database, config);
            return errors?.Any() == true;
        }, true, timeout: 60_000);
        Assert.True(value);

        await AssertDocs(store, marker);
    }

    private async Task AssertDocs(DocumentStore store, string marker)
    {
        var sw = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                using (var session = store.OpenAsyncSession())
                {
                    var tx1 = await session.LoadAsync<Transaction>("txs/2025-01-01");
                    Assert.Equal(marker, tx1.Summary.First().Notes); // didn't changed
                    var tx2 = await session.LoadAsync<Transaction>("txs/2025-01-02");
                    Assert.NotEqual(marker, tx2.Summary.First().Notes);
                }
                break;
            }
            catch (Exception e) when (e is EqualException or NotEqualException)
            {
                if (sw.Elapsed > TimeSpan.FromSeconds(15))
                    throw;
            }
        }
    }


    [RavenTheory(RavenTestCategory.Ai | RavenTestCategory.Etl)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Skip = "Takes too long")]
    public async Task ImgOver32MbRequest(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Categorize the expenses in the associated file";
        config.Collection = "Transactions";
        config.SampleObject = JsonConvert.SerializeObject(new
        {
            Summary = new[]
            {
                new {
                    Category = "Expense category (food | entertainment | utilities | education)",
                    TotalSpent = 10m,
                    TransctionCount = 5,
                    Notes = "General observations on this expense category based on the actual expenses (spend too much on takeout or fees are high on utility, etc)"
                }
            }
        });
        config.UpdateScript = @"this.Summary = $output.Summary;";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = $"const banana = '{BananaPngBase64}'; " + Environment.NewLine +
@"

if (this.UserId === 'users/1') 
{
    ai.genContext({
        Id: this.UserId,
        Date: this.Time,
        Location: this.City,
    })
    .withJpeg(loadAttachment('46mb.jpg'));
}
else{
    ai.genContext({
        Id: this.UserId,
        Date: this.Time,
        Location: this.City,
    })
    .withPng(banana);
}
"
        };

        await store.Maintenance.SendAsync(new AddGenAiOperation(config));

        var marker = store.Database + Guid.NewGuid();
        var markerSummary = new Summary[] { new Summary(string.Empty, 0, 0, marker) };

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Transaction("users/1", new DateTime(2025, 1, 1), "New York", markerSummary), "txs/2025-01-01");
            await session.StoreAsync(new Transaction("users/2", new DateTime(2025, 1, 2), "Netanya", markerSummary), "txs/2025-01-02");
            await using var bigJpeg = CreateBmpStream(46);

            session.Advanced.Attachments.Store("txs/2025-01-01", "46mb.jpg", bigJpeg);

            await session.SaveChangesAsync();
        }

        var value = await WaitForValueAsync(async () =>
        {
            var errors = await Etl.GetItemLoadErrorsAsync(store.Database, config);
            return errors?.Any() == true;
        }, true, timeout: 180_000);

        Assert.True(value);

        await AssertDocs(store, marker);
    }

    private static Stream CreateBmpStream(int sizeInMb)
    {
        if (sizeInMb <= 0)
            throw new ArgumentOutOfRangeException(nameof(sizeInMb));

        const int BmpHeaderSize = 14 + 40; // BITMAPFILEHEADER (14) + BITMAPINFOHEADER (40)
        long targetBytes = Math.Max(1, sizeInMb) * 1024L * 1024L;

        // Number of pixels we can store (4 bytes per pixel). Keep <= target to avoid overshooting.
        long maxPixels = Math.Max(1, (targetBytes - BmpHeaderSize) / 4);

        // Choose width/height close to a square, but ensure width*height <= maxPixels.
        int width = (int)Math.Ceiling(Math.Sqrt(maxPixels));
        if (width < 1)
            width = 1;
        int height = (int)(maxPixels / width);
        if (height < 1)
        { height = 1; width = (int)Math.Min(maxPixels, int.MaxValue); }

        long pixels = (long)width * height;
        long imageBytes = pixels * 4;
        long fileSize = BmpHeaderSize + imageBytes;

        var ms = new MemoryStream((int)fileSize);

        using (var bw = new BinaryWriter(ms, System.Text.Encoding.UTF8, leaveOpen: true))
        {
            // ---- BITMAPFILEHEADER (14 bytes) ----
            bw.Write((ushort)0x4D42);             // bfType = 'BM'
            bw.Write((uint)fileSize);             // bfSize
            bw.Write((ushort)0);                  // bfReserved1
            bw.Write((ushort)0);                  // bfReserved2
            bw.Write((uint)BmpHeaderSize);        // bfOffBits (pixel data offset)

            // ---- BITMAPINFOHEADER (40 bytes) ----
            bw.Write((uint)40);                   // biSize
            bw.Write((int)width);                 // biWidth
            bw.Write((int)(-height));             // biHeight (negative = top-down rows)
            bw.Write((ushort)1);                  // biPlanes
            bw.Write((ushort)32);                 // biBitCount (32bpp)
            bw.Write((uint)0);                    // biCompression = BI_RGB (no compression)
            bw.Write((uint)imageBytes);           // biSizeImage
            bw.Write((int)2835);                  // biXPelsPerMeter (~72 DPI)
            bw.Write((int)2835);                  // biYPelsPerMeter
            bw.Write((uint)0);                    // biClrUsed
            bw.Write((uint)0);                    // biClrImportant
        }

        // ---- Pixel data (BGRA top-down, stride = width * 4, already 4-byte aligned) ----
        var row = new byte[width * 4];
        for (int y = 0; y < height; y++)
        {
            RandomNumberGenerator.Fill(row);
            // Ensure alpha is non-zero for visibility (set to 255)
            for (int i = 3; i < row.Length; i += 4)
                row[i] = 255;
            ms.Write(row, 0, row.Length);
        }

        ms.Position = 0;
        return ms; // Stream contains a valid BMP file (not Base64)
    }

    [RavenTheory(RavenTestCategory.Ai | RavenTestCategory.Etl)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, Skip = "Takes too long")]
    public async Task Over1500ImagesRequest(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Categorize the expenses in the associated file";
        config.Collection = "Transactions";
        config.SampleObject = JsonConvert.SerializeObject(new
        {
            Summary = new[]
            {
                new {
                    Category = "Expense category (food | entertainment | utilities | education)",
                    TotalSpent = 10m,
                    TransctionCount = 5,
                    Notes = "General observations on this expense category based on the actual expenses (spend too much on takeout or fees are high on utility, etc)"
                }
            }
        });
        config.UpdateScript = @"this.Summary = $output.Summary;";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = $"const banana = '{BananaPngBase64}'; " + Environment.NewLine +
                     @"

if (this.UserId === 'users/1') 
{
    ai.genContext({
        Date: this.Date,
        Location: this.Location,
    })" + string.Join(string.Empty, Enumerable.Repeat(".withPng(banana)", 1600)) + ";" + // string.Join uses string builder
@"
}
else{
    ai.genContext({
        Date: this.Date,
        Location: this.Location,
    }).withPng(banana);
}
"

        };

        await store.Maintenance.SendAsync(new AddGenAiOperation(config));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Transaction("users/1", new DateTime(2025, 1, 1), "New York"), "txs/2025-01-01");
            await session.StoreAsync(new Transaction("users/2", new DateTime(2025, 1, 2), "Netanya"), "txs/2025-01-02");

            await session.SaveChangesAsync();
        }

        var value = await WaitForValueAsync(async () =>
        {
            var errors = await Etl.GetItemLoadErrorsAsync(store.Database, config);
            return errors?.Any() == true;
        }, true, timeout: 60_000);

        Assert.True(value);
    }

    private record Summary(string Category, decimal TotalSpent, int TransactionCount, string Notes);

    private record Transaction(string UserId, DateTime Time, string City, Summary[] Summary = null);

    private static MemoryStream BuildCsvStream(string titles, string[] rows, double targetMb = 40)
    {
        if (string.IsNullOrEmpty(titles))
            throw new ArgumentException("titles is required.", nameof(titles));
        if (rows == null || rows.Length == 0)
            throw new ArgumentException("rows must contain at least one item.", nameof(rows));

        var encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);
        long targetBytes = (long)(targetMb * 1024L * 1024L);

        var ms = new MemoryStream(capacity: (int)Math.Min(targetBytes, int.MaxValue));

        using (var writer = new StreamWriter(ms, encoding, bufferSize: 16 * 1024, leaveOpen: true))
        {
            writer.AutoFlush = true;
            writer.WriteLine(titles);

            int i = 0;
            while (ms.Length < targetBytes)
            {
                writer.WriteLine(rows[i]);
                i = (i + 1) % rows.Length;
            }
        }

        ms.Position = 0;
        return ms;
    }

    public static bool ValidateErrorsNotification(DocumentDatabase db, string[] errors1, string id = null)
    {
        using (db.NotificationCenter.GetStored(out var actions))
        {
            var jsonAlerts = actions.ToList();
            if (jsonAlerts.Count == 0)
                return false;

            foreach (var err in errors1)
            {
                var bjro = jsonAlerts.First().Json;

                if (bjro.TryGet("Details", out BlittableJsonReaderObject details) &&
                    details.TryGet("Errors", out BlittableJsonReaderArray errors) &&
                    errors.Length > 0)
                {
                    var firstErr = errors.First() as BlittableJsonReaderObject;
                    var contained = firstErr != null &&
                           (id == null || (firstErr.TryGet("DocumentId", out string documentId) && documentId == id)) &&
                           firstErr.TryGet("Error", out string error) && error.Contains(err);

                    if (contained == false)
                        return false;
                }
            }


            return true;
        }
    }

    private static string GetFileAsBase64(string fileName)
    {
        using var file = GetFileAsStream(fileName);

        using var memoryStream = new MemoryStream();

        using var transform = new ToBase64Transform();
        using var cryptoStream = new CryptoStream(file, transform, CryptoStreamMode.Read);
        cryptoStream.CopyTo(memoryStream);

        Span<byte> readOnlySpan = memoryStream.GetBuffer();
        return Encoding.UTF8.GetString(readOnlySpan[..(int)memoryStream.Length]);
    }

    private static Stream GetFileAsStream(string fileName)
    {
        var assembly = typeof(RavenDB_24648).Assembly;
        var resourceName = "SlowTests.Data.RavenDB_24648." + fileName;

        return assembly.GetManifestResourceStream(resourceName)
               ?? throw new FileNotFoundException($"Embedded resource not found: {resourceName}");
    }
}
