using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class LateBoundValue : RavenTestBase
    {
		public class SampleData : AggregateBaseEx<string>
		{

		}

		public class SampleData_Index : AbstractMultiMapIndexCreationTask<SampleData>
		{
			public SampleData_Index()
			{
				AddMapForAll<AggregateBase>(docs => from doc in docs
													select new
													{
														doc.Name
													});
				StoreAllFields(FieldStorage.Yes);
			}
		}

		public abstract class AggregateBaseEx<TType> : AggregateBase
		{
			public TType AnotherProp { get; set; }
		}
		public abstract class AggregateBase
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

        [Fact]
        public void CanIndexAndQuery()
        {
            using (var store = NewDocumentStore())
            {
                new SampleData_Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new SampleData
                    {
                        Name = "RavenDB",
                        AnotherProp = "AnotherProp"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<SampleData, SampleData_Index>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .FirstOrDefault();

                    Assert.Equal(result.Name, "RavenDB");
                }
            }
        }

        [Fact]
        public void CanLoadDoc()
        {
            using (var store = NewDocumentStore())
            {
                string id;
                using (var session = store.OpenSession())
                {
                    var sampleData = new SampleData
                        {
                            Name = "RavenDB", AnotherProp = "AnotherProp"
                        };
                    session.Store(sampleData);
                    id = sampleData.Id;
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Load<SampleData>(id);
                    
                    Assert.Equal(result.Name, "RavenDB");
                }
            }
        }
    }

 
}
