using System;
using Oracle.ManagedDataAccess.Client;
using Xunit;

namespace Tests.Infrastructure
{
    public class RequiresOracleSqlFactAttribute : FactAttribute
    {
        private static readonly Lazy<bool> IsOracleSqlAvailableLazy = new Lazy<bool>(() =>
        {
            try
            {
                using (var con = new OracleConnection(OracleTests.LocalConnectionWithTimeout))
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

        public static bool IsOracleSqlAvailable => IsOracleSqlAvailableLazy.Value;

        public RequiresOracleSqlFactAttribute()
        {
            if (IsOracleSqlAvailable == false)
                Skip = "Test requires Oracle database";
        }
    }
}
