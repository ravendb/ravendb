using System.Collections.Generic;

namespace Raven.Server.Integrations.PostgreSQL.Handlers
{
    public class PostgreSQLUsernamesList
    {
        public List<User> Users { get; set; }

        public PostgreSQLUsernamesList()
        {
            Users = new List<User>();
        }
    }

    public class User
    {
        public string Username { get; set; }
    }
}
