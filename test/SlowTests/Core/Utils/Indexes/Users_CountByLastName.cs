using System.Linq;

using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;

using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Core.Utils.Indexes
{
    public class Users_CountByLastName : AbstractIndexCreationTask<User>
    {
        public Users_CountByLastName()
        {
            Map = users => from u in users select new { Name = u.Name, LastName = u.LastName, Count = 1 };

            Reduce = results => from result in results
                                group result by new { result.Name, result.LastName } into g
                                select new { g.Key.Name, g.Key.LastName, Count = g.Sum(x => x.Count) };
        }
    }
}
