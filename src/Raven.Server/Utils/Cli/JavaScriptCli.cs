using System;
using System.IO;
using System.Text;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Utils.Cli
{
    internal sealed class JavaScriptCli
    {
        private const string ExecutionStr = "function ExecuteAdminScript(databaseInner){{ return (function(database){{ {0} }}).apply(this, [databaseInner]); }};";
        private const string ServerExecutionStr = "function ExecuteAdminScript(serverInner){{ return (function(server){{ {0} }}).apply(this, [serverInner]); }};";

        public bool CreateScript(TextReader reader, TextWriter writer, bool consoleColoring, DocumentDatabase database, RavenServer server)
        {
            var execString = database != null ? ExecutionStr : ServerExecutionStr;
            if (consoleColoring)
                Console.ForegroundColor = ConsoleColor.Cyan;
            writer.WriteLine();
            writer.WriteLine("Enter JavaScript:");

            if (consoleColoring)
                Console.ForegroundColor = ConsoleColor.DarkCyan;

            writer.WriteLine("(to cancel enter in new line 'cancel' or 'EXEC' to execute)");
            writer.WriteLine();

            var sb = new StringBuilder();

            if (consoleColoring)
                Console.ResetColor();

            AdminConsole = new AdminJsConsole(server, database);
            if (AdminConsole.Log.IsWarnEnabled)
            {
                var from = consoleColoring ? "the console CLI" : "a named pipe connection";
                AdminConsole.Log.Warn($"This operation was initiated through {from}");
            }
            while (true)
            {
                writer.Write(">>> ");
                if (consoleColoring == false)
                    writer.Write(RavenCli.GetDelimiterString(RavenCli.Delimiter.ReadLine));
                writer.Flush();

                var line = reader.ReadLine();
                if (line.Equals("cancel"))
                    return false;
                if (line.Equals("EXEC"))
                    break;

                sb.Append(line);
            }

            Script = sb.ToString();

            return true;
        }

        public string Script { get; set; }
        public AdminJsConsole AdminConsole { get; set; }
    }
}
