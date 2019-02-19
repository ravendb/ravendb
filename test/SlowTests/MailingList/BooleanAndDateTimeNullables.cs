using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class BooleanAndDateTimeNullables : RavenTestBase
    {
        private class ObjectWithNullables
        {
            public ObjectWithNullables()
            {
                TimeCollection = new List<DateTimeOffset>();
            }

            public string Name { get; set; }
            public bool? Excluded { get; set; }
            public DateTimeOffset? Time { get; set; }
            public ICollection<DateTimeOffset> TimeCollection { get; set; }
        }

        private class Raven20Style_NullableBoolean : AbstractIndexCreationTask<ObjectWithNullables>
        {
            public Raven20Style_NullableBoolean()
            {
                Map = objects => from o in objects
                                where !(o.Excluded ?? false)
                                select new
                                        {
                                            o.Name
                                        };
            }
        }

        private class Raven25Style_NullableBoolean : AbstractIndexCreationTask<ObjectWithNullables>
        {
            public Raven25Style_NullableBoolean()
            {
                Map = objects => from o in objects
                                where ((o.Excluded ?? false) == false)
                                select new
                                        {
                                            o.Name
                                        };
            }
        }


        private class Raven20Style_NullableDateTimeOffset : AbstractIndexCreationTask<ObjectWithNullables>
        {
            public Raven20Style_NullableDateTimeOffset()
            {
                Map = objects => from o in objects
                                select new
                                        {
                                            Times = o.TimeCollection.Any() ? o.TimeCollection.OrderByDescending(d => d).ToList() : new List<DateTimeOffset> { o.Time ?? DateTimeOffset.MinValue },
                                        };
            }
        }

        private class Raven25Style_NullableDateTimeOffset : AbstractIndexCreationTask<ObjectWithNullables>
        {
            public Raven25Style_NullableDateTimeOffset()
            {
                Map = objects => from o in objects
                                select new
                                        {
                                            Times = o.TimeCollection.Any() ? o.TimeCollection.OrderByDescending(d => d).ToList() : new List<DateTimeOffset> {(DateTimeOffset)(o.Time ?? DateTimeOffset.MinValue) },
                                        };
            }
        }


        private void TestIndexSetup(params AbstractIndexCreationTask<ObjectWithNullables>[] indexes)
        {
            using (var store = GetDocumentStore())
            {
                foreach (var index in indexes)
                {
                    index.Execute(store);
                }
            }			
        }

        [Fact]
        public void CanUseRaven20BoolIndex()
        {
            TestIndexSetup(new Raven20Style_NullableBoolean());
        }

        [Fact]
        public void CanUseRaven25BoolIndex()
        {
            TestIndexSetup(new Raven25Style_NullableBoolean());
        }

        [Fact]
        public void CanUseRaven20DateTimeIndex()
        {
            TestIndexSetup(new Raven20Style_NullableDateTimeOffset());
        }

        [Fact]
        public void CanUseRaven25DateTimeIndex()
        {
            TestIndexSetup(new Raven25Style_NullableDateTimeOffset());
        }
    }
}
