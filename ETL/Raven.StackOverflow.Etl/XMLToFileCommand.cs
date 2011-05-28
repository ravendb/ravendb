using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.StackOverflow.Etl.Posts;
using Raven.StackOverflow.Etl.Users;
using Rhino.Etl.Core;

namespace Raven.StackOverflow.Etl
{
    public class XMLToFileCommand : ICommand
    {
        public string CommandText { get { return "xml"; } }

        public string InputDirectory { get; private set; }
        public string OutputDirectory { get; set; }

        public void Run()
        {
            if (Directory.Exists("Docs"))
                Directory.Delete("Docs", true);
            Directory.CreateDirectory("Docs");

            var processes = new EtlProcess[]
            {
                new UsersProcess(InputDirectory, OutputDirectory),
                new BadgesProcess(InputDirectory, OutputDirectory),
                new PostsProcess(InputDirectory, OutputDirectory),
                new VotesProcess(InputDirectory, OutputDirectory),
                new CommentsProcess(InputDirectory, OutputDirectory)
            };
            Parallel.ForEach(processes, GenerateJsonDocuments);
        }

        private void GenerateJsonDocuments(EtlProcess process)
        {
            Console.WriteLine("Executing {0}", process);
            var sp = Stopwatch.StartNew();
            process.Execute();
            Console.WriteLine("Executed {0} in {1}", process, sp.Elapsed);
            var allErrors = process.GetAllErrors().ToArray();
            foreach (var exception in allErrors)
            {
                Console.WriteLine(exception);
            }
            if (allErrors.Length > 0)
            {
                throw new InvalidOperationException("Failed to execute process: " + process);
            }
        }

        public void LoadArgs(IEnumerable<string> remainingArgs)
        {
        }

        public void WriteHelp(TextWriter tw)
        {
            Console.WriteLine("Raven.StackOverflow.Etl.exe xml");
        }
    }
}
