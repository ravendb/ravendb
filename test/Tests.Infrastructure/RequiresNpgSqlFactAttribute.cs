using System;
using MySql.Data.MySqlClient;
using Npgsql;
using Xunit;

namespace Tests.Infrastructure
{
    public class RequiresNpgSqlFactAttribute : FactAttribute
    {
        private static readonly Lazy<bool> IsNpgSqlAvailableLazy = new Lazy<bool>(() =>
        {
            try
            {
                using (var con = new NpgsqlConnection(NpgSqlTests.LocalConnectionWithTimeout))
                {
                    con.Open();
                }

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        });

        public static bool IsNpgSqlAvailable => IsNpgSqlAvailableLazy.Value;

        public RequiresNpgSqlFactAttribute()
        {
            if (IsNpgSqlAvailable == false)
                Skip = "Test requires NpgSQL database";
        }
    }
}
