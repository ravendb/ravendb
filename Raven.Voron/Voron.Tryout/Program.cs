using BenchmarkDotNet;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Voron.Data.Tables;
using Voron.Platform.Posix;
using Voron.Tests;
using Voron.Tests.Tables;
using Xunit;

namespace Voron.Tryout
{
    public unsafe class Program : TableStorageTest
    {
        // [Params(32, 64, 512)]
        [Params(32, 64)]
        public int DataSize = 0;

        private string data;

        [Setup]
        public void Setup()
        {
            Random rnd = new Random();
            DataDir = $"data.{rnd.Next()}.test";

            data = new string('A', DataSize);

            using (var tx = Env.WriteTransaction())
            {
                _docsSchema.Create(tx);

                var docs = new Table<Documents>(_docsSchema, tx);

                var doc = new Documents { Etag = 1L, Key = "users/test", Data = data, Collection = "Users" };
                docs.Set(doc);
                docs.ReadByKey(new Slice("users/test"));

                tx.Commit();
            }
        }

        //[Benchmark]
        //[OperationsPerInvoke(100)]
        //public TableStorageTest InsertInTable()
        //{
        //    for (int i = 0; i < 100; i++)
        //    {
        //        using (var tx = Env.WriteTransaction())
        //        {
        //            var docs = new Table<DocumentsFields>(_docsSchema, tx);

        //            var structure = new Structure<DocumentsFields>(_docsSchema.StructureSchema)
        //                .Set(DocumentsFields.Etag, 1L)
        //                .Set(DocumentsFields.Key, "users/" + i)
        //                .Set(DocumentsFields.Data, data)
        //                .Set(DocumentsFields.Collection, "Users");
        //            docs.Set(structure);

        //            tx.Commit();
        //        }
        //    }

        //    return this;
        //}

        [Benchmark]
        public TableStorageTest InsertInTable()
        {
            using (var tx = Env.WriteTransaction())
            {
                var docs = new Table<Documents>(_docsSchema, tx);

                for (int i = 0; i < 100; i++)
                {
                    var doc = new Documents { Etag = 1L, Key = "users/" + i, Data = data, Collection = "Users" };
                    docs.Set(doc);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var docs = new Table<Documents>(_docsSchema, tx);

                for (int i = 0; i < 100; i++)
                {
                    var t = docs.ReadByKey(new Slice("users/" + i));
                }
            }

            return this;
        }

        public static void Main(string[] args)
        {
            var p = new Program();
            p.DataSize = 256;
            p.Setup();
            p.InsertInTable();

            //// Use reflection for a more maintainable way of creating the benchmark switcher,
            //// Benchmarks are listed in namespace order first (e.g. BenchmarkDotNet.Samples.CPU,
            //// BenchmarkDotNet.Samples.IL, etc) then by name, so the output is easy to understand
            //var benchmarks = Assembly.GetExecutingAssembly().GetTypes()
            //    .Where(t => t.GetMethods(BindingFlags.Instance | BindingFlags.Public)
            //                 .Any(m => m.GetCustomAttributes(typeof(BenchmarkAttribute), false).Any()))
            //    .OrderBy(t => t.Namespace)
            //    .ThenBy(t => t.Name)
            //    .ToArray();

            //var competitionSwitch = new BenchmarkCompetitionSwitch(benchmarks);
            //competitionSwitch.Run(args);
        }
    }
}