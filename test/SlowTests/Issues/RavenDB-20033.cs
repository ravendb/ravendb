using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20033 : RavenTestBase
{
    
    public RavenDB_20033(ITestOutputHelper output) : base(output)
    {
    }

    private string LoadDocumentsUrlGenerator(IDocumentStore store, int keys_range_min, int keys_range_max, string format)
    {
        StringBuilder str = new($"{store.Urls[0]}/databases/{store.Database}/streams/docs?&");
        for(int i = keys_range_min; i < keys_range_max; i++)
            str.Append($"id=users%2F{i}&");
        str.Append($"format={format}");
        return str.ToString();
    }

    private string ThrowStreamIsEmpty() => throw new InvalidDataException("stream is empty");
    
    [Fact]
    public async Task JsonlDocumentsLoadStreamReturnsCorrectData()
    {
        var store = GetDocumentStore();
        using var session = store.OpenSession();
        for (int i = 0; i < 10; i++)
        {
            session.Store(new User {Id = $"User/{i}", Name = $"Gracjan{i}"});
        }
        session.SaveChanges();
        using var client = new HttpClient();
        string jsonResult;
        await using (var stream = await client.GetStreamAsync(LoadDocumentsUrlGenerator(store, 0, 10, "json")))
        {
            using TextReader tr = new StreamReader(stream);
            jsonResult = await tr.ReadToEndAsync();
            Assert.NotEmpty(jsonResult);
        }
        var standardOutput = JsonConvert.DeserializeObject(jsonResult) as JObject;
        Assert.NotNull(standardOutput);

        await using var jsonlStream = await client.GetStreamAsync(LoadDocumentsUrlGenerator(store, 0, 10, "jsonl"));
        using var jsonlReader = new StreamReader(jsonlStream);
        
        var standardResults = standardOutput["Results"] as JArray;
        Assert.NotNull(standardResults);

        foreach (var child in standardResults.Children())
        {
            var standardResult = child as JObject;
            var jsonlResult = JsonConvert.DeserializeObject(await jsonlReader.ReadLineAsync() ?? ThrowStreamIsEmpty()) as JObject;
            Assert.NotNull(standardResult);
            Assert.NotNull(jsonlResult);
            
            Assert.Equal(standardResult["Name"], jsonlResult!["Name"]);
            Assert.Equal(standardResult["LastName"], jsonlResult["LastName"]);
            Assert.Equal(standardResult["AddressId"], jsonlResult["AddressId"]);
            Assert.Equal(standardResult["Count"], jsonlResult["Count"]);
            Assert.Equal(standardResult["@metadata"], jsonlResult["@metadata"]);
        }
        
        Assert.Empty(await jsonlReader.ReadLineAsync() ?? string.Empty);
    }
                
}
