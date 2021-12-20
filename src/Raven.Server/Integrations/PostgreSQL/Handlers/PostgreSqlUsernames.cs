using System.Collections.Generic;

namespace Raven.Server.Integrations.PostgreSQL.Handlers
{
    public class PostgreSqlUsernames
    {
        public List<PostgreSqlUsername> Users { get; set; }

        public PostgreSqlUsernames()
        {
            Users = new List<PostgreSqlUsername>();
        }
    }

    public class PostgreSqlUsername
    {
        public string Username { get; set; }
    }
}
