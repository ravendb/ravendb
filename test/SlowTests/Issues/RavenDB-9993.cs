using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9993 : RavenTestBase
    {
        public RavenDB_9993(ITestOutputHelper output) : base(output)
        {
        }

        private class Purchase
        {
#pragma warning disable 649,169
            public int? Quantity;
            public int QuantityInvoiced;
#pragma warning restore 649,169
        }

        [Fact]
        public void CanHaveArrayInMetadata()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Purchase(), "purchases/181972");
                    session.SaveChanges();
                }

                store.Operations.Send(new PatchOperation("purchases/181972", null, new PatchRequest
                {
                    Script = @"this[""@metadata""][""Object-Relations""] = [""ArticleSorts/108361/-/S20-070"",
                        ""suppliers/Import/ROYALFLOWERS"",
                        ""suppliers/Import/8714231212171"",
                        ""packagings/Base/535"",
                        ""features/base/6167"",
                        ""features/base/6503"",
                        ""features/base/5102"",
                        ""features/base/7950"",
                        ""features/base/4854"",
                        ""locations/1"",
                        ""Currencies/Base/2""]"
                }));

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Purchase>("purchases/181972");
                    session.SaveChanges();

                    var metadata = session.Advanced.GetMetadataFor(loaded);
                    Assert.True(metadata.TryGetValue("Object-Relations", out object arr));

                    var expected = new[]
                    {
                        "ArticleSorts/108361/-/S20-070",
                        "suppliers/Import/ROYALFLOWERS",
                        "suppliers/Import/8714231212171",
                        "packagings/Base/535",
                        "features/base/6167",
                        "features/base/6503",
                        "features/base/5102",
                        "features/base/7950",
                        "features/base/4854",
                        "locations/1",
                        "Currencies/Base/2"
                    };

                    Assert.Equal(expected, arr);

                }
            }
        }

        [Fact]
        public void CanAddArrayToMetadataViaClient()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Purchase(), "purchases/181972");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Purchase>("purchases/181972");
                    session.SaveChanges();

                    var metadata = session.Advanced.GetMetadataFor(loaded);
                    var obj = new[]
                    {
                        "ArticleSorts/108361/-/S20-070",
                        "suppliers/Import/ROYALFLOWERS",
                        "suppliers/Import/8714231212171",
                        "packagings/Base/535",
                        "features/base/6167",
                        "features/base/6503",
                        "features/base/5102",
                        "features/base/7950",
                        "features/base/4854",
                        "locations/1",
                        "Currencies/Base/2"
                    };

                    metadata.TryAdd("Object-Relations", obj);

                    session.SaveChanges();

                    Assert.True(metadata.TryGetValue("Object-Relations", out object arr));

                    Assert.Equal(obj, arr);

                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Purchase>("purchases/181972");
                    session.SaveChanges();

                    var metadata = session.Advanced.GetMetadataFor(loaded);
                    var obj = new List<string>
                    {
                        "ArticleSorts/108361/-/S20-070",
                        "suppliers/Import/ROYALFLOWERS",
                        "suppliers/Import/8714231212171",
                        "packagings/Base/535",
                        "features/base/6167",
                        "features/base/6503",
                        "features/base/5102",
                        "features/base/7950",
                        "features/base/4854",
                        "locations/1",
                        "Currencies/Base/2"
                    };
                    metadata.TryAdd("Object-Relations2", obj);

                    session.SaveChanges();

                    Assert.True(metadata.TryGetValue("Object-Relations2", out object arr));

                    Assert.Equal(obj, arr);
                }

            }
        }
    }
}
