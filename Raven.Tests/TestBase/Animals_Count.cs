using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Tests.TestBase
{
    public class Animals_Count : AbstractMultiMapIndexCreationTask<Animals_Count.AnimalCountResult>
    {
        public Animals_Count()
        {
            AddMapForAll<Animal>(animals => from animal in animals
                select new
                {
                    Type = MetadataFor(animal)["Raven-Entity-Name"],
                    Total = 1
                });

            Reduce = results => from result in results
                group result by result.Type
                into g
                select new
                {
                    Type = g.Key,
                    Total = g.Sum(x => x.Total)
                };
        }

        public class AnimalCountResult
        {
            public string Type { get; set; }
            public int Total { get; set; }
        }
    }
}
