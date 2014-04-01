using NDesk.Options;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.RavenFS;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Filesystem.CreateSample
{
    public class Program
    {
        private Uri uri;
        private DirectoryInfo directory;
        private string filesystem = string.Empty;
        private string filesystemLocation = "~/Filesystems";

        private IList<string> extras = new List<string>();

        public Program(string[] args)
        {
            /// Example command prompt call
            /// filesystem-createsample -server=http://localhost:8080 -filesystem=test -directory=Filesystem-simple


            var options = new OptionSet()
            {               
                {"s|server=", "Server Url.", v => uri = new Uri ( v, UriKind.Absolute ) },            
                {"d|directory=", "The directory we are going to import.", v => directory = new DirectoryInfo ( v ) },
                {"f|filesystem=", "The filesystem name.", v => filesystem = v.ToLower() },           
                {"dd|datadir=", "The data directory where files will be stored.", v => filesystemLocation = v.ToLower() },           
            };

            extras = options.Parse(args);    
        }

        public async Task Execute(string[] args)
        {
            var documentStore = new DocumentStore
            {
                Url = uri.ToString(),                
            }
            .Initialize();            

            var client = new RavenFileSystemClient(uri.ToString(), filesystem);

            Console.WriteLine("=== Available File Systems ===");

            bool doesFileSystemExists = false;
            foreach (string name in await client.Admin.GetFileSystemsNames())
            {
                Console.WriteLine(name);
                doesFileSystemExists |= name.ToLower() == filesystem;
            }

            if (!doesFileSystemExists)
            {
                var filesystemDocument = new DatabaseDocument() { Id = "Raven/FileSystems/" + filesystem };
                filesystemDocument.Settings["Raven/FileSystem/DataDir"] = Path.Combine(filesystemLocation, filesystem);

                await client.Admin.CreateFileSystemAsync(filesystemDocument, filesystem);
            }

            Console.WriteLine();            

            foreach ( var file in directory.GetFiles())
            {
                await client.UploadAsync(file.Name, file.OpenRead());
            }
        }

        static void Main(string[] args)
        {
            var program = new Program(args);
            program.Execute(args)
                   .Wait();
        }
    }
}
