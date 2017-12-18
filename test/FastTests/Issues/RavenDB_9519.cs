using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Smuggler;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_9519:RavenTestBase
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
                    await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions
                    {
                        FromCsv = true
                    }, tmpFile);
                    using (var session = store.OpenSession())
                    {
                        var res = session.Query<Company>().ToList();
                        Assert.Equal(2,res.Count);
                        Assert.True(res[0].Equals(res[1]));
                    }
                }
            }
            catch (Exception e)
            {
                
            }
            finally
            {
                if (tmpFile != null)
                {
                    File.Delete(tmpFile);
                }
            }
        }

        private Company _testCompany =  new Company
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
            Fax= "(26) 642-7012",
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
    }


}
