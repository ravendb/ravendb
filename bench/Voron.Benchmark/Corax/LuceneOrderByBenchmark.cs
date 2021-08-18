//using System;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnostics.Windows.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;
using Directory = System.IO.Directory;
using Version = Lucene.Net.Util.Version;

namespace Voron.Benchmark.Lucene
{

    //[DisassemblyDiagnoser]
    //[InliningDiagnoser(logFailuresOnly: true, filterByNamespace: true)]
    public class LuceneOrderByBenchmark
    {
        public virtual bool DeleteBeforeSuite { get; protected set; } = true;
        public virtual bool DeleteAfterSuite { get; protected set; } = true;
        public virtual bool DeleteBeforeEachBenchmark { get; protected set; } = false;


        [Params(1024, 2048, 4096, 16 * 1024)]
        //[Params(1024)]
        public int BufferSize { get; set; }

        [Params(16, 64, 256)]
        public int TakeSize { get; set; }


        /// <summary>
        /// Path to store the benchmark database into.
        /// </summary>
        public const string Path = Configuration.Path;

        /// <summary>
        /// This is the job configuration for storage benchmarks. Changing this
        /// will affect all benchmarks done.
        /// </summary>
        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(new Job
                {
                    Environment =
                    {
                        Runtime = CoreRuntime.Core50,
                        Platform = BenchmarkDotNet.Environments.Platform.X64,
                        Jit = Jit.RyuJit
                    },
                    Run =
                    {
                        LaunchCount = 1,
                        WarmupCount = 1,
                        IterationCount = 1,
                        InvocationCount = 1,
                        UnrollFactor = 1
                    },
                    // TODO: Next line is just for testing. Fine tune parameters.
                });

                // Exporters for data
                AddExporter(GetExporters().ToArray());
                // Generate plots using R if %R_HOME% is correctly set
                AddExporter(RPlotExporter.Default);

                AddColumn(StatisticColumn.AllStatistics);

                AddValidator(BaselineValidator.FailOnError);
                AddValidator(JitOptimizationsValidator.FailOnError);

                AddAnalyser(EnvironmentAnalyser.Default);
            }
        }

        public LuceneOrderByBenchmark()
        {
            if (DeleteBeforeSuite)
            {
                DeleteStorage();
            }

            if (!DeleteBeforeEachBenchmark)
            {
                GenerateData();
            }
        }

        ~LuceneOrderByBenchmark()
        {
            if (DeleteAfterSuite)
            {
                DeleteStorage();
            }
        }

        public static void GenerateData()
        {
            var directoryInfo = new DirectoryInfo(Path);
            if (directoryInfo.Exists == false)
                directoryInfo.Create();
            using SimpleFSDirectory dir = new SimpleFSDirectory(directoryInfo);
            using var writer = new IndexWriter(dir,
                new StandardAnalyzer(Version.LUCENE_30),
                true,
                IndexWriter.MaxFieldLength.UNLIMITED,
                null);
            

            var doc = new Document();
            doc.Add(new Field("Name", "Arava", Field.Store.NO, Field.Index.ANALYZED));
            doc.Add(new Field("Family", "Eini", Field.Store.NO, Field.Index.ANALYZED));
            doc.Add(new Field("Age", 12.ToString(), Field.Store.NO, Field.Index.ANALYZED));
            doc.Add(new Field("Type", "Dog", Field.Store.NO, Field.Index.ANALYZED));
            writer.AddDocument(doc, null);


            doc = new Document();
            doc.Add(new Field("Name", "Phoebe", Field.Store.NO, Field.Index.ANALYZED));
            doc.Add(new Field("Family", "Eini", Field.Store.NO, Field.Index.ANALYZED));
            doc.Add(new Field("Age", 7.ToString(), Field.Store.NO, Field.Index.ANALYZED));
            doc.Add(new Field("Type", "Dog", Field.Store.NO, Field.Index.ANALYZED));
            writer.AddDocument(doc, null);



            for (int i = 0; i < 100_000; i++)
            {
                doc = new Document();
                doc.Add(new Field("Name", "Dog #" + i, Field.Store.NO, Field.Index.ANALYZED));
                doc.Add(new Field("Family", "families/" + (i % 1024), Field.Store.NO, Field.Index.ANALYZED));
                doc.Add(new Field("Age", (1%15).ToString(), Field.Store.NO, Field.Index.ANALYZED));
                doc.Add(new Field("Type", "Dog", Field.Store.NO, Field.Index.ANALYZED));
                writer.AddDocument(doc, null);
            }
            
            writer.Commit(null);

        }

        [GlobalSetup]
        public virtual void Setup()
        {
            if (DeleteBeforeEachBenchmark)
            {
                DeleteStorage();
                GenerateData();
            }

            _ids = new long[BufferSize];
            _indexSearcher = new IndexSearcher(new SimpleFSDirectory(new DirectoryInfo(Path)), null);

            _query = new QueryParser(Version.LUCENE_30, "", new StandardAnalyzer(Version.LUCENE_30)).Parse("Type: Dog AND Age: 1*");
            _sort = new Sort(new SortField("Age", SortField.STRING));
        }


        [GlobalCleanup]
        public virtual void Cleanup()
        {
          
        }

        private void DeleteStorage()
        {
            if (!Directory.Exists(Path))
                return;

            for (var i = 0; i < 10; ++i)
            {
                try
                {
                    Directory.Delete(Path, true);
                    break;
                }
                catch (DirectoryNotFoundException)
                {
                    break;
                }
                catch (Exception)
                {
                    Thread.Sleep(20);
                }
            }
        }

        private long[] _ids;
        private IndexSearcher _indexSearcher;
        private Query _query;
        private Sort _sort;

        [Benchmark]
        public void OrderByRuntimeQuery()
        {
             _indexSearcher.Search(_query, null, TakeSize, _sort,null);
        }       
    }
}
