using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Raven.Client.Client;
using Raven.Client.Indexes;
using Raven.Storage.Esent;

namespace etobi.MemLeakTest
{
    class Program
    {
        static void Main(string[] args)
        {
            ForceDeleteDirectory("Data");
            using (var documentStore = new EmbeddableDocumentStore())
            {
                documentStore.Configuration.DefaultStorageTypeName = typeof (TransactionalStorage).AssemblyQualifiedName;
                documentStore.Configuration.DataDirectory = "Data";
                documentStore.Initialize();

                for (var i = 0; i < 0; i++)
                {
                    documentStore.DatabaseCommands.PutIndex("bar" + i, new IndexDefinition<Bar>
                    {
                        Map = docs => from doc in docs select new { doc.Name }
                    });
                }

                var iteration = 0;
                while (true)
                {
                    using (var session = documentStore.OpenSession())
                    {
                        Console.WriteLine("Iteration: #{0}, Memory usage: {1:0,0}", iteration++, GC.GetTotalMemory(true));
                        session.Query<Foo>().Where(x => x.Name == "something").Take(1).FirstOrDefault();
                        for (var i = 0; i < 100; i++)
                        {
                            session.Store(new Foo());
                        }
                        session.SaveChanges();
                    }
                }
            }
        }

        private static void ForceDeleteDirectory(string path)
        {
            if (!Directory.Exists(path)) return;

            var root = new DirectoryInfo(path);

            var subDirs = new Stack<DirectoryInfo>();
            subDirs.Push(root);
            while (subDirs.Count > 0)
            {
                var subDir = subDirs.Pop();
                subDir.Attributes = subDir.Attributes & ~(FileAttributes.Archive | FileAttributes.ReadOnly | FileAttributes.Hidden);
                foreach (var dir in subDir.GetDirectories())
                {
                    subDirs.Push(dir);
                }
                foreach (var file in subDir.GetFiles())
                {
                    file.Attributes = file.Attributes & ~(FileAttributes.Archive | FileAttributes.ReadOnly | FileAttributes.Hidden);
                    file.Delete();
                }
            }
            root.Delete(true);
        }
    }

    public class Foo
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    public class Bar
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
