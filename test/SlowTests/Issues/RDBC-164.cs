using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RDBC_164 : RavenTestBase
    {
        [Fact]
        public async Task DeepIndexingArray()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps =
                    {
                        @"from m in docs.Motorbikes
select new
{
    EngineNumber = m.EngineNumber,
    Vin = m.Vin,
    RegistrationNumber = m.RegistrationNumber,
    PolicyNumbers = (
        from p in m.DriverGroups.SelectMany(g => g.Value).SelectMany(d => d.PolicyNumbers)
        select p)
}"
                    },
                    Name = "Motorbikes/PolicySearch"
                }));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Motorbike
                    {
                        EngineNumber = "DT982345WQ3452",
                        Vin = "R398G4587",
                        RegistrationNumber = "BW90TXGP",
                        RegisteredProvince = "GP",
                        Year = 2008,
                        DriverGroups = new Dictionary<string, List<Driver>>
                    {
                        {
                            "Primary",
                            new List<Driver>
                            {
                                new Driver
                                {
                                    DriverId = 4600400,
                                    LicenseNumber = "79936R8391783",
                                    PolicyNumbers = new List<long> { 47712117, 48011696, 48062187 }
                                }
                            }
                        },
                        {
                            "Owner",
                            new List<Driver>
                            {
                                new Driver
                                {
                                    DriverId = 4489421,
                                    LicenseNumber = "478TR03304909",
                                    PolicyNumbers = new List<long> { 479722098, 488216011 }
                                }
                            }
                        },
                    }
                    });

                    await session.StoreAsync(new Motorbike
                    {
                        EngineNumber = "V3M89875W00P52",
                        Vin = "D3Y8G4117",
                        RegistrationNumber = "DR12WHGP",
                        RegisteredProvince = "GP",
                        Year = 2011,
                        DriverGroups = new Dictionary<string, List<Driver>>
                    {
                        {
                            "Primary",
                            new List<Driver>
                            {
                                new Driver
                                {
                                    DriverId = 4298377,
                                    LicenseNumber = "65T18TY144403",
                                    PolicyNumbers = new List<long> { 481973771 }
                                }
                            }
                        }
                    }
                    });

                    await session.StoreAsync(new Motorbike
                    {
                        EngineNumber = "QF066930Y19PA",
                        Vin = "G00PT1773",
                        RegistrationNumber = "CT44ZBGP",
                        RegisteredProvince = "GP",
                        Year = 2011,
                        DriverGroups = new Dictionary<string, List<Driver>>
                    {
                        {
                            "Primary",
                            new List<Driver>
                            {
                                new Driver
                                {
                                    DriverId = 4520016,
                                    LicenseNumber = "49722W2110103",
                                    PolicyNumbers = new List<long> { 48071101, 48093371, 48221209 }
                                }
                            }
                        },
                        {
                            "Alternate",
                            new List<Driver>
                            {
                                new Driver
                                {
                                    DriverId = 4520021,
                                    LicenseNumber = "81374K2169001",
                                    PolicyNumbers = new List<long> { 483313101 }
                                },
                                new Driver
                                {
                                    DriverId = 4509941,
                                    LicenseNumber = "6A88BT214486",
                                    PolicyNumbers = new List<long> { 482212470, 480881221 }
                                }
                            }
                        }
                    }
                    });

                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var results = session
                        .Advanced
                        .AsyncDocumentQuery<Motorbike>("Motorbikes/PolicySearch")
                        .WhereIn("PolicyNumbers", new object[] { 481973771 })
                        .AndAlso()
                        .OpenSubclause()
                        .WhereLucene("EngineNumber", "V3M89*").OrElse()
                        .WhereLucene("Vin", "V3M89*")
                        .CloseSubclause()
                        .WaitForNonStaleResults();

                    Assert.NotEmpty(await results.ToListAsync());
                }
            }
        }

        public class Motorbike
        {
            public string Id => "Motorbike/" + this.RegistrationNumber;

            public string RegistrationNumber { get; set; }

            public string Vin { get; set; }

            public string EngineNumber { get; set; }

            public string RegisteredProvince { get; set; }

            public int Year { get; set; }

            public Dictionary<string, List<Driver>> DriverGroups { get; set; }
        }

        public class Driver
        {
            public long DriverId { get; set; }

            public string LicenseNumber { get; set; }

            public List<long> PolicyNumbers { get; set; }
        }
    }
}
