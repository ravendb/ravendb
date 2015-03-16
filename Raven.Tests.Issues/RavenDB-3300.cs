using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3300 : RavenTestBase
    {
        public class Car
        {
            public String Model { get; set;}
            public String Color { get; set; }
            public int Year { get; set;}

        }
        [Fact]
        public void ExposeResultEtagInStatistics()
        {
            using (var store = NewDocumentStore())
            {
                var session = store.OpenSession();
               session.Store(new Car { Model = "Toyota", Color = "Blue", Year = 2005});
               session.Store(new Car { Model = "Mazda", Color = "White", Year = 2010 });
               session.Store(new Car { Model = "Ford", Color = "Blue", Year = 2012 });
               session.Store(new Car { Model = "BMW", Color = "Silver", Year = 2015 });
               session.Store(new Car { Model = "Toyota Corola", Color = "Red", Year = 2008 });
               session.Store(new Car { Model = "Fiat", Color = "Blue", Year = 2005 });
                session.SaveChanges();
                RavenQueryStatistics stats;
                var query = session.Query<Car>()
                                .Statistics(out stats)
                                .Where(x => x.Color == "Blue")
                                .ToList();
                var resultEtag = stats.ResultEtag;
                Assert.NotNull(resultEtag);
                Assert.NotEqual(resultEtag, Etag.Empty);
            }
        }
    }
}
