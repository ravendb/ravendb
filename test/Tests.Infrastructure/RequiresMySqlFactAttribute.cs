using System;
using System.Runtime.InteropServices;
using MySql.Data.MySqlClient;
using Xunit;

namespace Tests.Infrastructure
{
    public class RequiresMySqlFactAttribute : FactAttribute
    {
        public RequiresMySqlFactAttribute()
        {
            try
            {
                using (var con = new MySqlConnection(MySqlTests.LocalConnection))
                {
                    con.Open();
                }
            }
            catch (Exception e)
            {
                Skip = "Test requires MySQL database";
            }
        }
    }
}
