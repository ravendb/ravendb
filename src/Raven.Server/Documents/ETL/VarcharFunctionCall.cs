using System.Data;

namespace Raven.Server.Documents.ETL
{
    public class VarcharFunctionCall
    {
        public static string AnsiStringType = DbType.AnsiString.ToString();
        public static string StringType = DbType.String.ToString();

        public DbType Type { get; set; }
        public object Value { get; set; }
        public int Size { get; set; }

        private VarcharFunctionCall()
        {

        }
    }
}
