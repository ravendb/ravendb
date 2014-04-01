using System;
using System.Linq;

using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
    public class NullableDateTimeOffsetFromDateTimeParse : RavenTestBase
    {
        public class DataEntry
        {
            public string Id { get; set; }

            public DateTimeOffset? NullableDateTimeOffsetProperty { get; set; }
        }

        public class IndexWithDateTimeParse : AbstractIndexCreationTask<DataEntry>
        {
            public IndexWithDateTimeParse()
            {
                Map = dataEntries => from entry in dataEntries
                                     select new
                                     {
                                         NullableDateTimeOffsetProperty = entry.NullableDateTimeOffsetProperty <= DateTime.Parse("1/1/1900") ?
                                                   (DateTime?)null : entry.NullableDateTimeOffsetProperty.GetValueOrDefault().DateTime
                                     };
            }
        }

        [Fact]

        public void Query_on_index_with_nullable_DateTimeOffset_property_and_DateTimeParse_in_index_should_work()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new DataEntry { NullableDateTimeOffsetProperty = DateTime.Now });
                    session.SaveChanges();
                }

                new IndexWithDateTimeParse().Execute(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<DataEntry, IndexWithDateTimeParse>()
                          .Customize(customization => customization.WaitForNonStaleResults())
                          .ToList();

                    AssertNoIndexErrors(store);
                    Assert.Equal(1, results.Count);
                }
            }
        }
    }
}