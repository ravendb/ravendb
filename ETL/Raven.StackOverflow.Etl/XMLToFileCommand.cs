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
        public bool Force { get; set; }

        public void Run()
        {
            if (Directory.Exists(OutputDirectory) && Force)
            {
                Directory.Delete(OutputDirectory, true);
            }

            if (!Directory.Exists(OutputDirectory))
                Directory.CreateDirectory(OutputDirectory);

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
            if (remainingArgs.Count() != 2)
                throw new ArgumentException("Incorrect number of arguments");

            if (!Directory.Exists(InputDirectory))
                throw new ArgumentException("Input directory was missing");

            if (!Force && Directory.Exists(OutputDirectory))
            {
                if (Directory.GetDirectories(OutputDirectory, "*").Any() || Directory.GetFiles(OutputDirectory, "*").Any())
                {
                    throw new ArgumentException("Output directory should be empty");
                }
            }
        }

        public void WriteHelp(TextWriter tw)
        {
            Console.WriteLine("Raven.StackOverflow.Etl.exe xml <inputDirectory> <outputDirectory>");
        }
    }
}
