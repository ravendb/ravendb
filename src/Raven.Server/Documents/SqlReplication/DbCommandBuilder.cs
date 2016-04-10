namespace Raven.Server.Documents.SqlReplication
{
    public class DbCommandBuilder
    {
        public string QuoteIdentifier(string unquotedIdentifier)
        {
            return "[" + unquotedIdentifier + "]";
        }
    }
}