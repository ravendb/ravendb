using System.Linq;
using Raven.Client.Indexes;
using SlowTests.Core.Utils.Entities;

namespace SlowTests.Core.Utils.Transformers
{
    public class Companies_NameTransformer : AbstractTransformerCreationTask<Company>
    {
        public class Result
        {
            public string Name { get; set; }
        }

        public Companies_NameTransformer()
        {
            TransformResults = companies => from c in companies
                                            select new
                                            {
                                                c.Name
                                            };
        }
    }
}