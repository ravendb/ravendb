using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Indexes;
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


        private async Task TestIndexSetup(params AbstractIndexCreationTask<ObjectWithNullables>[] indexes)
        {
            using (var store = await GetDocumentStore())
            {
                foreach (var index in indexes)
                {
                    index.Execute(store);
                }
            }			
        }

        [Fact]
        public async Task CanUseRaven20BoolIndex()
        {
            await TestIndexSetup(new Raven20Style_NullableBoolean());
        }

        [Fact]
        public async Task CanUseRaven25BoolIndex()
        {
            await TestIndexSetup(new Raven25Style_NullableBoolean());
        }

        [Fact(Skip = "https://github.com/dotnet/roslyn/issues/12045")]
        public async Task CanUseRaven20DateTimeIndex()
        {
            await TestIndexSetup(new Raven20Style_NullableDateTimeOffset());
        }

        [Fact(Skip = "https://github.com/dotnet/roslyn/issues/12045")]
        public async Task CanUseRaven25DateTimeIndex()
        {
            await TestIndexSetup(new Raven25Style_NullableDateTimeOffset());
        }
    }
}
