using System.Linq;
using Raven.Client.Documents.Indexes;
using SlowTests.Core.Utils.Entities;

namespace SlowTests.Core.Utils.Indexes
{
    public class Users_ByCity : AbstractIndexCreationTask<User>
    {
        public class Result
        {
            public string City { get; set; }
        }

        public Users_ByCity()
        {
            Map = users => from user in users
                           let address = LoadDocument<Address>(user.AddressId)
                           select new
                           {
                               Test = "a",
                               City = address.City
                           };
        }
    }
}
