using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Utils;
using Sparrow.Json;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_9519 : RavenTestBase
    {
        [Fact]
        public async Task NestedObjectShouldBeExportedAndImportedProperly()
        {
            string tmpFile = null;
            try
            {
                using (var store = GetDocumentStore())
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(_testCompany, "companies/1");
                        session.SaveChanges();
                    }

                    var client = new HttpClient();
                    var stream = await client.GetStreamAsync($"{store.Urls[0]}/databases/{store.Database}/streams/queries?query=From%20companies");
                    tmpFile = Path.GetTempFileName();
                    using (var file = File.Create(tmpFile))
                    {
                        stream.CopyTo(file);
                        await file.FlushAsync();
                    }

                    using (var commands = store.Commands())
                    {
                        var getOperationIdCommand = new GetNextOperationIdCommand();
                        await commands.RequestExecutor.ExecuteAsync(getOperationIdCommand, commands.Context);
                        var operationId = getOperationIdCommand.Result;

                        using (var fileStream = File.OpenRead(tmpFile))
                        {
                            var csvImportCommand = new CsvImportCommand(fileStream, null, operationId);

                            await commands.ExecuteAsync(csvImportCommand);

                            var operation = new Operation(commands.RequestExecutor, () => store.Changes(), store.Conventions, operationId);

                            await operation.WaitForCompletionAsync();
                        }
                    }

                    using (var session = store.OpenSession())
                    {
                        var res = session.Query<Company>().ToList();
                        Assert.Equal(2, res.Count);
                        Assert.True(res[0].Equals(res[1]));
                    }
                }
            }
            finally
            {
                if (tmpFile != null)
                    IOExtensions.DeleteFile(tmpFile);
            }
        }

        private readonly Company _testCompany = new Company
        {
            ExternalId = "WOLZA",
            Name = "Wolski  Zajazd",
            Contact = new Contact
            {
                Name = "Zbyszek Piestrzeniewicz",
                Title = "Owner"
            },
            Address = new Address
            {
                City = "Warszawa",
                Country = "Poland",
                Line1 = "ul. Filtrowa 68",
                Line2 = null,
                Location = new Location
                {
                    Latitude = 52.21956300000001,
                    Longitude = 20.985878
                },
                PostalCode = "01-012",
                Region = null
            },
            Phone = "(26) 642-7012",
            Fax = "(26) 642-7012",
        };

        private class Company
        {
            public string Id { get; set; }
            public string ExternalId { get; set; }
            public string Name { get; set; }
            public Contact Contact { get; set; }
            public Address Address { get; set; }
            public string Phone { get; set; }
            public string Fax { get; set; }
            public override bool Equals(object obj)
            {
                if (!(obj is Company other))
                    return false;

                if (ReferenceEquals(this, other))
                    return true;

                return ExternalId == other.ExternalId && Name == other.Name && Contact?.Name == other.Contact?.Name
                       && Contact?.Title == other.Contact?.Title && Address?.City == other.Address?.City && Address?.Country == other.Address?.Country
                       && Address?.Line1 == other.Address?.Line1 && Address?.Line2 == other.Address?.Line2 && Address?.PostalCode == other.Address?.PostalCode
                       && Address?.Region == other.Address?.Region && Address?.Location?.Latitude == other.Address?.Location?.Latitude
                       && Address?.Location?.Longitude == other.Address?.Location?.Longitude && Phone == other.Phone && Fax == other.Fax;
            }
        }

        private class Address
        {
            public string Line1 { get; set; }
            public string Line2 { get; set; }
            public string City { get; set; }
            public string Region { get; set; }
            public string PostalCode { get; set; }
            public string Country { get; set; }
            public Location Location { get; set; }
        }

        private class Location
        {
            public double Latitude { get; set; }
            public double Longitude { get; set; }
        }

        private class Contact
        {
            public string Name { get; set; }
            public string Title { get; set; }
        }

        private class CsvImportCommand : RavenCommand
        {
            private readonly Stream _stream;
            private readonly string _collection;
            private readonly long _operationId;

            public override bool IsReadRequest => false;

            public CsvImportCommand(Stream stream, string collection, long operationId)
            {
                _stream = stream ?? throw new ArgumentNullException(nameof(stream));

                _collection = collection;
                _operationId = operationId;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/smuggler/import/csv?operationId={_operationId}&collection={_collection}";

                var form = new MultipartFormDataContent
                {
                    {new StreamContent(_stream), "file", "name"}
                };

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = form
                };
            }
        }
    }
}
