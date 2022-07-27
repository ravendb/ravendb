using System;
using Npgsql;
using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace Tests.Infrastructure
{
    public class RequiresNpgSqlFactAttribute : FactAttribute
    {
        public RequiresNpgSqlFactAttribute()
        {
            if (NpgSqlConnectionString.Instance.CanConnect() == false)
                Skip = "Test requires NpgSQL database";
        }
    }
}
