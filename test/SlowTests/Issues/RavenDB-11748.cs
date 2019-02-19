using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11748 : RavenTestBase
    {
        public class Car
        {
            public string Id { get; set; }
            public string ModelId { get; set; }
            public string Name { get; set; }
        }

        public class CarsIndex : AbstractIndexCreationTask<Car>
        {
            public CarsIndex()
            {
                Map = docs =>
                    from doc in docs
                    select new { doc.Name };
            }
        }
        public class ModelConfig
        {
            public static readonly string DocumentId = "ModelConfig";
            public string Id { get; set; }
            public List<Model> Models { get; set; }
        }

        public class Model
        {
            public string ModelId { get; set; }
            public string Name { get; set; }
        }

        public class CarOutput
        {
            public string CarName { get; set; }
            public string ModelName { get; set; }
        }


        [Fact]
        public void CanUseSingleOrDefaultInQueries()
        {
            using (var store = GetDocumentStore())
            {
                new CarsIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new ModelConfig
                    {
                        Id = ModelConfig.DocumentId,
                        Models = new List<Model>
                            {
                                new Model {ModelId = "Models-1", Name = "Ford"},
                                new Model {ModelId = "Models-2", Name = "Chevy"},
                                new Model {ModelId = "Models-3", Name = "Honda"}
                            }
                    });

                    session.Store(new Car
                    {
                        ModelId = "Models-1",
                        Name = "Car1"
                    });
                    session.Store(new Car
                    {
                        ModelId = "Models-2",
                        Name = "Car2"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);


                using (var session = store.OpenSession())
                {
                    var results =
                        from result in session.Query<Car, CarsIndex>().Where(x => x.Name == "Car1")
                        let modelConfig = RavenQuery.Load<ModelConfig>("ModelConfig")
                        let model = modelConfig.Models.SingleOrDefault(x => x.ModelId == result.ModelId)
                        //     let model = modelConfig.Models.FirstOrDefault(x => x.ModelId == result.ModelId) // Changing this to FirstOrDefault works
                        select new CarOutput
                        {
                            CarName = result.Name,
                            ModelName = model != null ? model.Name : "UNKNOWN_MODEL"
                        };

                    var e = Assert.Throws<NotSupportedException>(() => results.ToList());
                    Assert.Equal("Unable to translate 'SingleOrDefault' to RQL operation because this method is not familiar to the RavenDB query provider.", e.Message);

                }
            }
        }
    }
}
