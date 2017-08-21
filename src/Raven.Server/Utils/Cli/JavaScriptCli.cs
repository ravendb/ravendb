using System;
using System.IO;
using System.Text;
using Raven.Server.Documents;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Utils.Cli
{
    internal class JavaScriptCli
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

            writer.WriteLine("(to cancel enter in new line 'cancel' or 'done' to execute)");
            writer.WriteLine("(  script will be executed automatically if found valid   )");
            writer.WriteLine();

            var sb = new StringBuilder();

            if (consoleColoring)
                Console.ResetColor();

            AdminConsole = database != null ? new AdminJsConsole(database) : new AdminJsConsole(server);
            if (AdminConsole.Log.IsOperationsEnabled)
            {
                var from = consoleColoring ? "the console cli" : "a named pipe connection";
                AdminConsole.Log.Operations($"This operation was initiated through {from}");
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
                if (line.Equals("done"))
                    break;

                sb.Append(line);

                var adminJsScript = new AdminJsScript { Script = sb.ToString() };
                bool hadErrors = false;
                try
                {
                    AdminConsole.GetEngine(adminJsScript, execString);
                }
                catch
                {
                    hadErrors = true;
                }
                if (hadErrors == false)
                    break;
            }

            Script = sb.ToString();

            return true;
        }

        public string Script { get; set; }
        public AdminJsConsole AdminConsole { get; set; }
    }
}
