using System.Collections.Generic;

namespace Raven.Server.Integrations.PostgreSQL.Handlers
{
    public class PostgreSqlUsernamesList
    {
        public List<User> Users { get; set; }

        public PostgreSqlUsernamesList()
        {
            Users = new List<User>();
        }
    }

    public class User
    {
        public string Username { get; set; }
    }
}
