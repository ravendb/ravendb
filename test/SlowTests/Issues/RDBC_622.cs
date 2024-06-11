using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Extensions;
using Xunit;
using Xunit.Abstractions;
using SlowTests.Core.Utils.Entities;

namespace SlowTests.Issues;

public class RDBC_622 : RavenTestBase
{
    private char a = 'a';
    private char z = 'z';

    public RDBC_622(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task JsonlStreamReturnsCorrectData()
    {
        using var store = await PrepareDataForTest();
        using var client = new HttpClient().WithConventions(store.Conventions);
        string jsonResult;
        await using (var stream = await client.GetStreamAsync(UrlGenerator(store, "")))
        {
            using TextReader tr = new StreamReader(stream);
            jsonResult = await tr.ReadToEndAsync();
            Assert.NotEmpty(jsonResult);
        }

        var standardOutput = JsonConvert.DeserializeObject(jsonResult) as JObject;
        Assert.NotNull(standardOutput);
        
        await using var jsonlStream = await client.GetStreamAsync(UrlGenerator(store, "jsonl"));
        using var jsonlReader = new StreamReader(jsonlStream);
        
        var jsonlOutput = JsonConvert.DeserializeObject(await jsonlReader.ReadLineAsync() ?? ThrowStreamIsEmpty()) as JObject;
        Assert.NotNull(jsonlOutput);
        jsonlOutput = jsonlOutput["Stats"] as JObject;
        Assert.NotNull(jsonlOutput);
        
        
        Assert.Equal(standardOutput["IsStale"], jsonlOutput["IsStale"]);
        Assert.Equal(standardOutput["IndexName"], jsonlOutput["IndexName"]);
        Assert.Equal(standardOutput["TotalResults"], jsonlOutput["TotalResults"]);

        var standardResults = standardOutput["Results"] as JArray;
        Assert.NotNull(standardResults);

        foreach (var child in standardResults.Children())
        {
            var standardResult = child as JObject;
            Assert.NotNull(standardResult);
            
            var jsonlResultItem = (JsonConvert.DeserializeObject(await jsonlReader.ReadLineAsync() ?? ThrowStreamIsEmpty()) as JObject)?["Item"] as JObject;
            Assert.Equal(standardResult["Name"], jsonlResultItem!["Name"]);
            Assert.Equal(standardResult["LastName"], jsonlResultItem["LastName"]);
            Assert.Equal(standardResult["AddressId"], jsonlResultItem["AddressId"]);
            Assert.Equal(standardResult["Count"], jsonlResultItem["Count"]);
            Assert.Equal(standardResult["@metadata"], jsonlResultItem["@metadata"]);
        }
        
        Assert.Empty(await jsonlReader.ReadLineAsync() ?? string.Empty);
    }

    private string ThrowStreamIsEmpty() => throw new InvalidDataException("stream is empty");
    
    private string UrlGenerator(IDocumentStore store, string format) =>
        $"{store.Urls[0]}/databases/{store.Database}/streams/queries?query=from index \'Users/CoolCount\'&format={format}";
    
    private async Task<IDocumentStore> PrepareDataForTest()
    {
        var store = GetDocumentStore();
        using var session = store.OpenSession();
        for (var i = 0; i < 10; i++)
        {
            session.Store(new User {Id = $"User/{i}", Name = $"{a}", LastName = $"{z}", Count = i});
            a++;
            z--;
        }

        session.SaveChanges();

        await store.Maintenance.SendAsync(new PutIndexesOperation(new[]
        {
            new IndexDefinition
            {
                Maps =
                {
                    @"from user in docs.Users
                                    select new
                                    {
                                        user.Name,
                                        user.LastName,
                                        CoolCount = user.Count * 2
                                    }"
                },
                Name = "Users/CoolCount"
            }
        }));
        Indexes.WaitForIndexing(store);
        
        return store;
    }
}
