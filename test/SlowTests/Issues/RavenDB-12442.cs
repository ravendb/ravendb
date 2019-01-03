using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12442 : RavenTestBase
    {
        [Theory]
        [InlineData(Raven.Client.Constants.Documents.Indexing.Fields.NullValue)]
        [InlineData(Raven.Client.Constants.Documents.Indexing.Fields.EmptyString)]
        public void Can_convert_stored_null_value_and_empty_string(string str)
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var otherEnt = new OtherEnt("test1");
                    session.Store(otherEnt);

                    session.Store(new Ent1(otherEnt.Id, 1, EnumTest.Item1, str));
                    session.Store(new Ent2(otherEnt.Id, 2, EnumTest.Item2, str));
                    session.SaveChanges();
                }

                new SampleIndex().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = (from r in session.Query<SampleIndex.Result, SampleIndex>()
                        let other = RavenQuery.Load<OtherEnt>(r.IdOtherEnt)
                        select new SampleDto2
                        {
                            Id = r.Id,
                            NameOtherEnt = other.Name,
                            PropBase = r.PropBase,
                            Prop1 = r.Prop1,
                            Prop2 = r.Prop2,
                            Str = r.Str
                        }).ToList();

                    Assert.Equal(2, list.Count);

                    Assert.Equal(1, list[0].PropBase);
                    Assert.Equal(EnumTest.Item1, list[0].Prop1);
                    Assert.Equal(null, list[0].Prop2);
                    Assert.Equal(str, list[0].Str);

                    Assert.Equal(2, list[1].PropBase);
                    Assert.Equal(null, list[1].Prop1);
                    Assert.Equal(EnumTest.Item2, list[1].Prop2);
                    Assert.Equal(str, list[1].Str);
                }
            }
        }

        [Theory]
        [InlineData(Raven.Client.Constants.Documents.Indexing.Fields.NullValue)]
        [InlineData(Raven.Client.Constants.Documents.Indexing.Fields.EmptyString)]
        public void Can_convert_store_and_get_null_value_and_empty_string(string str)
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var otherEnt = new OtherEnt("test1");
                    session.Store(otherEnt);

                    session.Store(new Ent1(otherEnt.Id, 1, EnumTest.Item1, str));
                    session.Store(new Ent2(otherEnt.Id, 2, EnumTest.Item2, str));
                    session.SaveChanges();
                }

                new SampleIndex().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<SampleIndex.Result, SampleIndex>()
                        .ProjectInto<SampleDto>()
                        .ToList();

                    Assert.Equal(2, list.Count);

                    Assert.Equal(1, list[0].PropBase);
                    Assert.Equal(EnumTest.Item1, list[0].Prop1);
                    Assert.Equal(null, list[0].Prop2);
                    Assert.Equal(str, list[0].Str);

                    Assert.Equal(2, list[1].PropBase);
                    Assert.Equal(null, list[1].Prop1);
                    Assert.Equal(EnumTest.Item2, list[1].Prop2);
                    Assert.Equal(str, list[0].Str);
                }
            }
        }
    }

    public class SampleIndex : AbstractMultiMapIndexCreationTask<SampleIndex.Result>
    {
        public class Result
        {
            public string Id { get; set; }

            public string IdOtherEnt { get; set; }

            public int PropBase { get; set; }

            public EnumTest? Prop1 { get; set; }

            public EnumTest? Prop2 { get; set; }

            public string Str { get; set; }
        }

        public SampleIndex()
        {
            AddMap<Ent1>(
                items => (from ent in items
                        select new Result
                        {
                            Id = ent.Id,
                            IdOtherEnt = ent.IdOtherEnt,
                            PropBase = ent.PropBase,
                            Prop1 = ent.Prop1,
                            Prop2 = null,
                            Str = ent.Str
                        }
                    )
            );

            AddMap<Ent2>(
                items => (from ent in items
                        select new Result
                        {
                            Id = ent.Id,
                            IdOtherEnt = ent.IdOtherEnt,
                            PropBase = ent.PropBase,
                            Prop1 = null,
                            Prop2 = ent.Prop2,
                            Str = ent.Str
                        }
                    )
            );

            StoreAllFields(FieldStorage.Yes);
        }
    }

    public enum EnumTest
    {
        Item1,
        Item2
    }

    public class OtherEnt
    {
        public OtherEnt(string name)
        {
            Name = name;
        }

        public string Id { get; set; }

        public string Name { get; set; }
    }

    public class Ent1 : EntBase
    {
        public Ent1(string idOtherEnt, int propBase, EnumTest prop1, string str) : base(idOtherEnt, propBase, str)
        {
            Prop1 = prop1;
        }

        public EnumTest Prop1 { get; set; }
    }

    public class Ent2 : EntBase
    {
        public Ent2(string idOtherEnt, int propBase, EnumTest prop2, string str) : base(idOtherEnt, propBase, str)
        {
            Prop2 = prop2;
        }

        public EnumTest Prop2 { get; set; }
    }

    public class EntBase
    {
        public EntBase(string idOtherEnt, int propBase, string str)
        {
            IdOtherEnt = idOtherEnt;
            PropBase = propBase;
            Str = str;
        }

        public string Id { get; set; }

        public string IdOtherEnt { get; set; }

        public int PropBase { get; set; }

        public string Str { get; set; }
    }

    public class SampleDto
    {
        public string Id { get; set; }

        public int PropBase { get; set; }

        public EnumTest? Prop1 { get; set; }

        public EnumTest? Prop2 { get; set; }

        public string Str { get; set; }
    }

    public class SampleDto2
    {
        public string Id { get; set; }

        public string NameOtherEnt { get; set; }

        public int PropBase { get; set; }

        public EnumTest? Prop1 { get; set; }

        public EnumTest? Prop2 { get; set; }

        public string Str { get; set; }
    }
}
