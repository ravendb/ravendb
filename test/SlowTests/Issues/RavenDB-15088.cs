using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15088 : RavenTestBase
    {
        public RavenDB_15088(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public void QueryByDictionaryValues()
        {
            using (var store = GetDocumentStore())
            {
                var ingredientId = "Ingredient/1";

                var obj = new CompositionIngredient { IngredientId = ingredientId };
                using (var session = store.OpenSession())
                {
                    var ration = new Ration
                    {
                        Composition = new Dictionary<string, CompositionIngredient>
                        {
                            { "Composition/1", obj }
                        }
                    };

                    session.Store(ration);
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    // works
                    var q1 = session.Query<Ration>()
                        .Where(r => r.Composition.Any(ri => ri.Value == obj));
                    var r1 = q1.ToList();
                    Assert.Equal(1, r1.Count);
                    
                    // doesn't work
                    var q2 = session.Query<Ration>()
                        .Where(r => r.Composition.Any(ri => ri.Value.IngredientId == ingredientId));
                    var r2 = q2.ToList();
                    Assert.Equal(1, r2.Count);

                    // doesn't work
                    var q3 = session.Query<Ration>()
                        .Where(r => r.Composition.Values.Any(ri => ri.IngredientId == ingredientId));

                    var r3 = q3.ToList();
                    Assert.Equal(1, r3.Count);
                    
                    // doesn't work
                    var q4 = session.Query<Ration>()
                        .Where(r => r.Composition.Keys.Any(ri => ri ==  "Composition/1"));

                    var r4 = q3.ToList();
                    Assert.Equal(1, r4.Count);
                }
            }
        }

        public sealed class Ration
        {
            public Dictionary<string, CompositionIngredient> Composition { get; set; }
            public string Name { get; set; }
        }

        public class CompositionIngredient
        {
            public string IngredientId { get; set; }
        }
    }
}
