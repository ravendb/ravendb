using System;

namespace Raven.Server.Documents.SqlReplication
{
    public class DbCommandBuilder
    {
        public string Start, End;

        public string QuoteIdentifier(string unquotedIdentifier)
        {
            return Start + unquotedIdentifier + End;
        }
      
    }
}