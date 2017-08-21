using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace Raven.Server.Documents.ETL.Providers.SQL.Simulation
{
    public class TableQuerySummary
    {
        public string TableName { get; set; }
        public CommandData[] Commands { get; set; }


        public class CommandData
        {
            public string CommandText { get; set; }
            public KeyValuePair<string, object>[] Params { get; set; }
        }

        public static TableQuerySummary GenerateSummaryFromCommands(string tableName, IEnumerable<DbCommand> commands)
        {
            var tableQuerySummary = new TableQuerySummary();
            tableQuerySummary.TableName = tableName;
            tableQuerySummary.Commands =
                commands
                    .Select(x => new CommandData
                    {
                        CommandText = x.CommandText,
                        Params = x.Parameters.Cast<DbParameter>().Select(y => new KeyValuePair<string, object>(y.ParameterName, y.Value)).ToArray()
                    }).ToArray();

            return tableQuerySummary;
        }
    }
}