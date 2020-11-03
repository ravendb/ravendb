using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_10734 : RavenTestBase
    {
        public RavenDB_10734(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Complex_object_should_generate_csharp_class_properly()
        {
            using (var store = GetDocumentStore())
            {
                var requestExecutor = store.GetRequestExecutor();

                using (var context = JsonOperationContext.ShortTermSingleUse())
                using (var stringStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(ComplexDocument)))
                using (var blittableJson = await context.ReadForDiskAsync(stringStream, "Reading of foo/bar"))
                {
                    requestExecutor.Execute(new PutDocumentCommand("foo/bar", null, blittableJson), context);
                }

                var url = $"{store.Urls[0]}/databases/{store.Database}/docs/class?id=foo/bar";
                var responseAsString = await SendGetAndReadString(url);

                Assert.DoesNotContain("NotSupportedException", responseAsString);
                Assert.Contains("public class Item", responseAsString);
            }
        }

        public async Task<string> SendGetAndReadString(string url)
        {
            using (var client = new HttpClient())
                return await client.GetStringAsync(url);
        }

        private const string ComplexDocument = @"
        {
            ""ExtendedProductInformation"": {
                ""ExtendedDescription"": {
                    ""DefaultValue"": ""Z"",
                    ""Translations"": {
                        ""1033"": ""Z""
                    }
                },
                ""MainSpecifications"": [
                    {
                        ""SpecCategory"": """",
                        ""SpecName"": ""P"",
                        ""SpecValue"": ""Z"",
                        ""SpecDisplayOrder"": 1
                    },
                    {
                        ""SpecCategory"": """",
                        ""SpecName"": ""M"",
                        ""SpecValue"": ""5"",
                        ""SpecDisplayOrder"": 11
                    },
                    {
                        ""SpecCategory"": """",
                        ""SpecName"": ""D"",
                        ""SpecValue"": ""1"",
                        ""SpecDisplayOrder"": 9
                    }
                ],
                ""ExtendedSpecifications"": [
                    {
                        ""SpecCategory"": ""G"",
                        ""SpecName"": ""H"",
                        ""SpecValue"": ""1"",
                        ""SpecDisplayOrder"": 4
                    },
                    {
                        ""SpecCategory"": ""G"",
                        ""SpecName"": ""W"",
                        ""SpecValue"": ""1"",
                        ""SpecDisplayOrder"": 2
                    }
                ],
                ""DownloadableContents"": [
                    {
                        ""Description"": ""M"",
                        ""Url"": ""https://not_a_domain_at_all.never"",
                        ""MimeType"": ""application/xml""
                    }
                ],
                ""Images"": [
                    {
                        ""ImageId"": ""b"",
                        ""Weight"": 70,
                        ""Versions"": [
                            {
                                ""Url"": ""https://not_a_domain_at_all.neve.jpg"",
                                ""Size"": ""Thumbnail""
                            },
                            {
                                ""Url"": ""https://not_a_domain_at_all.neve.jpg"",
                                ""Size"": ""Medium""
                            }
                        ]
                    }
                ],
                ""KeySellingPointsUrl"": ""https://foobar/foobar/foobar.xml"",
                ""ProductFeaturesUrl"": null,
                ""ProductDisplayName"": null,
                ""ProductDescription"": null
            },
            ""Ancestries"": [
                {
                    ""Ancestors"": [
                        {
                            ""Id"": ""categories/3"",
                            ""Name"": ""I""
                        },
                        {
                            ""Id"": ""categories/4"",
                            ""Name"": ""N""
                        },
                        {
                            ""Id"": ""categories/5"",
                            ""Name"": ""N""
                        }
                    ]
                },
                {
                    ""Ancestors"": [
                        {
                            ""Id"": ""categories/5"",
                            ""Name"": ""N""
                        },
                        {
                            ""Id"": ""categories/9"",
                            ""Name"": ""Net""
                        }
                    ]
                }
            ],
            ""Sources"": {
                ""Data"": {
                    ""sigma"": [],
                    ""prosys"": []
                }
            },
            ""ProductMediaId"": ""mega-data-id!!"",
            ""ManufacturingPartNo"": ""Z"",
            ""Manufacturer"": ""Z!"",
            ""GPManufacturer"": ""Zs"",
            ""GPManufacturerId"": ""Z0"",
            ""DisplayName"": {
                ""DefaultValue"": ""Zy"",
                ""Translations"": {
                    ""1033"": ""Zy""
                }
            },
            ""Description"": {
                ""DefaultValue"": ""Z"",
                ""Translations"": {
                    ""1033"": ""Z""
                }
            },
            ""@metadata"": {
                ""@collection"": ""Items"",
                ""Raven-Clr-Type"": ""TestProject.Item, TestProject""
            }
        }
        ";
    }
}
