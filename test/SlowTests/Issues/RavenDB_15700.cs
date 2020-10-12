using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;
using Raven.Server.Documents.Indexes.Debugging;
using Sparrow.Json;
using Raven.Server.Json;

namespace SlowTests.Issues
{
    public class RavenDB_15700 : RavenTestBase
    {
        public RavenDB_15700(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanGetMapReduceIndexDebugTrees()
        {
            using (var store = GetDocumentStore())
            {
                SetupData(store);

                WaitForIndexing(store);

                var searchedVakanzId = "Vakanz/89d3d971-a227-430e-97ba-613799253a37";

                var db = await GetDatabase(store.Database);

                var index = db.IndexStore.GetIndex(new TestIndexVakanz_WithFiltering().IndexName);

                WaitForUserToContinueTheTest(store);

                using (var context = JsonOperationContext.ShortTermSingleUse())
                using (var writer = new BlittableJsonTextWriter(context, new MemoryStream()))
                using (index.GetReduceTree(new[] { searchedVakanzId, "bewerbung/000d7605-a581-4f3e-8c73-dc2eb4e3f95c" }, out var trees))
                {
                    // must not throw

                    writer.WriteReduceTrees(trees);
                }
            }
        }

        private static void SetupData(IDocumentStore store)
        {
            new TestIndexVakanz_WithFiltering().Execute(store);

            using (var _session = store.OpenSession())
            {
                var bewerberStatus = ReadAllLines("RavenDB_15700.BewerberStatus.csv");
                var bewerbung = ReadAllLines("RavenDB_15700.Bewerbung.csv");
                var vakanz = ReadAllLines("RavenDB_15700.Vakanz.csv");

                foreach (var line in bewerberStatus.Skip(1))
                {
                    var fields = line.Split(",");
                    var newEntity = new BewerberStatus() { Id = fields[0], Status = fields[1] };
                    _session.Store(newEntity);
                }

                foreach (var line in bewerbung.Skip(1))
                {
                    // @id,VakanzId,BewerberStatusId
                    var fields = line.Split(",");
                    var newEntity = new Bewerbung() { Id = fields[0], VakanzId = fields[1], BewerberStatusId = fields[2] };
                    _session.Store(newEntity);
                }

                foreach (var line in vakanz.Skip(1))
                {
                    // @id,SoftDeleted
                    var fields = line.Split(",");
                    var newEntity = new Vakanz() { Id = fields[0], SoftDeleted = fields[1].Equals("true", StringComparison.OrdinalIgnoreCase) };
                    _session.Store(newEntity);
                }


                _session.Store(new Bewerbung { Id = "Bewerbung/1", VakanzId = "Vakanz/DoesNotExist", BewerberStatusId = "BewerberStatus/1" });

                _session.Advanced.WaitForIndexesAfterSaveChanges();
                _session.SaveChanges();
            }
        }

        private class Vakanz
        {
            public string Id { get; set; }
            public string SomeProperty { get; set; }
            public bool SoftDeleted { get; set; }
        }

        private class Bewerbung
        {
            public string Id { get; set; }
            public string VakanzId { get; set; }
            public string BewerberStatusId { get; set; }
        }

        private class BewerberStatus
        {
            public string Id { get; set; }
            public string Status { get; set; }
            public object SomeOtherProperty { get; set; }

        }

        private static IEnumerable<string> ReadAllLines(string name)
        {
            using (var stream = typeof(RavenDB_15700).Assembly.GetManifestResourceStream("SlowTests.Data." + name))
            using (var reader = new StreamReader(stream))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    yield return line;
                }
            }
        }

        private class IndexResultModel
        {
            public string VakanzId { get; set; }
            public bool SoftDeleted { get; set; }
            public bool IstVakanz { get; set; }

            public List<Bewerbung> Bewerbungen { get; set; }

            public class Bewerbung
            {
                public RavenDB_15700.BewerberStatus BewerberStatus { get; set; }
            }
        }

        private class TestIndexVakanz_WithFiltering : AbstractMultiMapIndexCreationTask<IndexResultModel>
        {
            public TestIndexVakanz_WithFiltering()
            {
                AddMap<RavenDB_15700.Vakanz>(vakanzen => from v in vakanzen
                                                         where v.SoftDeleted == false
                                                         select new
                                                         {
                                                             IstVakanz = true,
                                                             VakanzId = v.Id,
                                                             SoftDeleted = v.SoftDeleted,
                                                             Bewerbungen = new IndexResultModel.Bewerbung[0],
                                                             Bewerbungen_BewerberStatus_Status = default(string),

                                                         });


                // The reason for "IstVakanz" is that i only want "Bewerbung"-Dokuments that still exist and are not softdeleted
                // There might be some "Bewerbung" with not existing "Vakanz" or softdeleted Vakanz
                AddMap<RavenDB_15700.Bewerbung>(bewerbung => from b in bewerbung
                                                             let bewerberStatus = LoadDocument<RavenDB_15700.BewerberStatus>(b.BewerberStatusId)
                                                             select new
                                                             {
                                                                 IstVakanz = false,
                                                                 VakanzId = b.VakanzId,
                                                                 SoftDeleted = false,
                                                                 Bewerbungen = new IndexResultModel.Bewerbung[] { new IndexResultModel.Bewerbung { BewerberStatus = bewerberStatus } },
                                                                 Bewerbungen_BewerberStatus_Status = bewerberStatus.Status,
                                                             });



                Reduce = results => from result in results
                                    where result.SoftDeleted == false
                                    group result by result.VakanzId
                    into g
                                    let first = g.FirstOrDefault(e => e.IstVakanz) // I just want Results with existing "Vakanz"
                                    where first.IstVakanz // with some other data, the .First() was not enough, so i had to add some extra where
                                    select new
                                    {
                                        SoftDeleted = false,
                                        IstVakanz = first.IstVakanz,
                                        VakanzId = first.VakanzId,
                                        Bewerbungen = g.SelectMany(e => e.Bewerbungen),
                                        Bewerbungen_BewerberStatus_Status = g.SelectMany(e => e.Bewerbungen).Select(e => e.BewerberStatus.Status).Distinct(),
                                    };

            }
        }
    }
}
