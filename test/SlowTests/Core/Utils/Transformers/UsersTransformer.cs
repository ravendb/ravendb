using System.Linq;

using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;

using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Core.Utils.Transformers
{
    public class UsersTransformer : AbstractTransformerCreationTask<User>
    {
        public class Result
        {
            public string PassedParameter { get; set; }
        }

        public UsersTransformer()
        {
            TransformResults = results => from result in results
                                          let key = ParameterOrDefault("Key", "LastName").Value<string>()
                                          select new { PassedParameter = key };
        }
    }
}
