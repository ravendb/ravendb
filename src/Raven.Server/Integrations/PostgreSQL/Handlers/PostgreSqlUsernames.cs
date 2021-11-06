using System.Collections.Generic;

namespace Raven.Server.Integrations.PostgreSQL.Handlers
{
    public class PostgreSqlUsernames
    {
        public List<User> Users { get; set; }

        public PostgreSqlUsernames()
        {
            Users = new List<User>();
        }
    }

    public class User
    {
        public string Username { get; set; }
    }
}
