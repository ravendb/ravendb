using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Corax;
using Corax.Querying;
using Corax.Mappings;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using Sparrow.Server;
using Sparrow.Threading;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace Voron.Benchmark.Corax
{

    //[DisassemblyDiagnoser(maxDepth: 900, printSource:true, exportHtml: true, exportDiff: true)]
    //[InliningDiagnoser(logFailuresOnly: true, allowedNamespaces: new[] { "Corax" })]
    public class OrderByBenchmark
    {
        public StorageEnvironment Env;
        public virtual bool DeleteBeforeSuite { get; protected set; } = true;
        public virtual bool DeleteAfterSuite { get; protected set; } = true;
        public virtual bool DeleteBeforeEachBenchmark { get; protected set; } = false;


        [Params(1024, 2048, 4096, 16 * 1024)]        
        //[Params(1024)]
        public int BufferSize { get; set; }

        [Params(16, 64, 256)]
        //[Params(16)]
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

        public OrderByBenchmark()
        {
            if (DeleteBeforeSuite)
            {
                DeleteStorage();
            }

            if (!DeleteBeforeEachBenchmark)
            {
                Env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(Path));
                GenerateData(Env);
            }
        }

        ~OrderByBenchmark()
        {
            if (!DeleteBeforeEachBenchmark)
            {
                Env.Dispose();
            }

            if (DeleteAfterSuite)
            {
                DeleteStorage();
            }
        }

        private static void GenerateData(StorageEnvironment env)
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexFieldsMapping fields = CreateFieldsMapping(bsc);

            using (var writer = new IndexWriter(env, fields, SupportedFeatures.All))
            {
                {
                    using var builder = writer.Index("items/1"u8);
                    builder.Write(0, Encoding.UTF8.GetBytes("Arava"));
                    builder.Write(1, Encoding.UTF8.GetBytes("Eini"));
                    builder.Write(2, Encoding.UTF8.GetBytes(12L.ToString()), 12L, 12D);
                    builder.Write(3, Encoding.UTF8.GetBytes("Dog"));
                    builder.EndWriting();
                }

                {
                    using var builder = writer.Index("items/2"u8);
                    builder.Write(0, Encoding.UTF8.GetBytes("Phoebe"));
                    builder.Write(1, Encoding.UTF8.GetBytes("Eini"));
                    builder.Write(2, Encoding.UTF8.GetBytes(7.ToString()), 7L, 7D);
                    builder.Write(3, Encoding.UTF8.GetBytes("Dog"));
                    builder.EndWriting();
                }

                for (int i = 0; i < 100_000; i++)
                {
                    using var builder = writer.Index("items/e"u8);
                    builder.Write(0, Encoding.UTF8.GetBytes("Dog #" + i));
                    builder.Write(1, Encoding.UTF8.GetBytes("families/" + (i % 1024)));
                    var age = i % 15;
                    builder.Write(2, Encoding.UTF8.GetBytes(age.ToString()), age, age);
                    builder.Write(3, Encoding.UTF8.GetBytes("Dog"));
                    builder.EndWriting();
                }

                writer.Commit();
            }
        }

        private static IndexFieldsMapping CreateFieldsMapping(ByteStringContext bsc)
        {
            Slice.From(bsc, "Name", ByteStringType.Immutable, out var nameSlice);
            Slice.From(bsc, "Family", ByteStringType.Immutable, out var familySlice);
            Slice.From(bsc, "Age", ByteStringType.Immutable, out var ageSlice);
            Slice.From(bsc, "Type", ByteStringType.Immutable, out var typeSlice);

            using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(0, nameSlice)
                .AddBinding(1, familySlice)
                .AddBinding(2, ageSlice)
                .AddBinding(3, typeSlice);
            using var fields = builder.Build();
            return fields;
        }

        [GlobalSetup]
        public virtual void Setup()
        {
            if (DeleteBeforeEachBenchmark)
            {
                DeleteStorage();
                Env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(Path));
                GenerateData(Env);
            }

            _ids = new long[BufferSize];
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            _indexSearcher = new IndexSearcher(Env, CreateFieldsMapping(bsc));


            _bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            Slice.From(_bsc, "Type", ByteStringType.Immutable, out _typeSlice);
            Slice.From(_bsc, "Dog", ByteStringType.Immutable, out _dogSlice);
            Slice.From(_bsc, "Age", ByteStringType.Immutable, out _ageSlice);
            Slice.From(_bsc, "1", ByteStringType.Immutable, out _ageValueSlice);
        }


        [GlobalCleanup]
        public virtual void Cleanup()
        {
            if (DeleteBeforeEachBenchmark)
            {
                Env.Dispose();
            }
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
        private ByteStringContext _bsc;
        private Slice _typeSlice;
        private Slice _dogSlice;
        private Slice _ageSlice;
        private Slice _ageValueSlice;

        [Benchmark]
        public void OrderByRuntimeQuery()
        {            
            var typeTerm = _indexSearcher.TermQuery(_typeSlice, _dogSlice);
            var ageField = FieldMetadata.Build(_ageSlice, default, 2, default, default);
            var ageTerm = _indexSearcher.StartWithQuery(ageField, _ageValueSlice);
            var andQuery = _indexSearcher.And(typeTerm, ageTerm);
            var query = _indexSearcher.OrderBy(andQuery, new OrderMetadata(ageField, true, MatchCompareFieldType.Sequence), take: TakeSize);           

            Span<long> ids = _ids;
            while (query.Fill(ids) != 0)
                ;
        }       
    }
}
