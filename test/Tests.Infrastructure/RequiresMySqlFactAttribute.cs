using System;
using MySql.Data.MySqlClient;
using Xunit;

namespace Tests.Infrastructure
{
    public class RequiresMySqlFactAttribute : FactAttribute
    {
        private static readonly Lazy<bool> IsMySqlAvailableLazy = new Lazy<bool>(() =>
        {
            try
            {
                using (var con = new MySqlConnection(MySqlTests.LocalConnectionWithTimeout))
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

        public static bool IsMySqlAvailable => IsMySqlAvailableLazy.Value;

        public RequiresMySqlFactAttribute()
        {
            if (IsMySqlAvailable == false)
                Skip = "Test requires MySQL database";
        }
    }
}
