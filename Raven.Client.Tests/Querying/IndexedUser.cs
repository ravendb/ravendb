using System;

namespace Raven.Client.Tests.Querying
{
    public class IndexedUser
    {
        public int Age { get; set; }
        public DateTime Birthday { get; set; }
        public string Name { get; set; }
        public string Email { get; set; }
    }
}