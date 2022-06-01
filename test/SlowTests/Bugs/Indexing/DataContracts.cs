using System.Collections.Generic;
using System.Runtime.Serialization;
using FastTests;
using Xunit;
using System.Linq;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Indexing
{
    public class DataContracts : RavenTestBase
    {
        public DataContracts(ITestOutputHelper output) : base(output)
        {
        }


        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void RespectsNameOnDataMemberAttribute(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new DataContractClass { Name = "tname" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<DataContractClass>()
                        .Where(tc => tc.Name.Equals("tname"))
                        .FirstOrDefault();

                    Assert.NotNull(result);
                    Assert.Equal("tname", result.Name);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void RespectsNameOnDataMemberAttributeWithNestedCollection(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new WrapperClass
                    {
                        Children = new List<DataContractClass>
                        {
                            new DataContractClass{ Name = "tname"}
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<WrapperClass>()
                        .Where(wc => wc.Children.Any(c => c.Name.Equals("tname")))
                        .FirstOrDefault();

                    Assert.NotNull(result);
                    Assert.Equal("tname", result.Children.FirstOrDefault().Name);
                }
            }
        }

        [DataContract]
        private class WrapperClass
        {
            [DataMember]
            public string Id { get; set; }

            [DataMember(Name="offspring")]
            public ICollection<DataContractClass> Children { get; set; }
        }

        [DataContract]
        private class DataContractClass
        {
            [DataMember]
            public string Id { get; set; }

            [DataMember(Name="n")]
            public string Name { get; set; }
        }
    }
}
