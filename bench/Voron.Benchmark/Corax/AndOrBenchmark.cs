﻿using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnostics.Windows.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Corax;
using Corax.Querying;
using Corax.Mappings;
using Raven.Server.Documents.Queries.Parser;
using Sparrow.Server;
using Sparrow.Threading;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace Voron.Benchmark.Corax
{
    [DisassemblyDiagnoser]
    [InliningDiagnoser(logFailuresOnly: true, filterByNamespace: true)]
    public class AndOrBenchmark
    {
        protected StorageEnvironment Env;
        public virtual bool DeleteBeforeSuite { get; protected set; } = true;
        public virtual bool DeleteAfterSuite { get; protected set; } = true;
        public virtual bool DeleteBeforeEachBenchmark { get; protected set; } = false;


        [Params(16, 64, 256, 1024)]
        public int BufferSize { get; set; }

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

        public AndOrBenchmark()
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

        ~AndOrBenchmark()
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
                    using var builder = writer.Index("Arava");
                    
                   builder.Write(0, "Arava"u8);
                   builder.Write(1, "Eini"u8);
                   builder.Write(2, Encoding.UTF8.GetBytes(12L.ToString()), 12L, 12D);
                   builder.Write(3, "Dog"u8);
                   builder.EndWriting();
                }

                {
                    using var builder = writer.Index("Phoebe");
                    builder.Write(0, "Phoebe"u8);
                    builder.Write(1, "Eini"u8);
                    builder.Write(2, Encoding.UTF8.GetBytes(7.ToString()), 7L, 7D);
                    builder.Write(3, "Dog"u8);
                    builder.EndWriting();
                }

                for (int i = 0; i < 10_000; i++)
                {
                    using var builder = writer.Index("Dog #" + i);
                    builder.Write(0, Encoding.UTF8.GetBytes("Dog #" + i));
                    builder.Write(1, Encoding.UTF8.GetBytes("families/" + (i % 1024)));
                    var age = i % 17;
                    builder.Write(2, Encoding.UTF8.GetBytes(age.ToString()), age, age);
                    builder.Write(3, "Dog"u8);
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

            var parser = new QueryParser();
            parser.Init("from Dogs where Type = 'Dog' and Age = '15'");
            _queryDefinition = new QueryDefinition("Name", parser.Parse());

            _ids = new long[BufferSize];
        }

        protected QueryDefinition _queryDefinition;

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

        [Benchmark]
        public void RuntimeQuery()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var indexSearcher = new IndexSearcher(Env, CreateFieldsMapping(bsc));

            var identityReader = indexSearcher.TermsReaderFor("id()");
            var typeTerm = indexSearcher.TermQuery("Type", "Dog");
            var familyTerm = indexSearcher.TermQuery("Age", "15");
            var query = indexSearcher.And(typeTerm, familyTerm);

            int read = 0;
            Span<long> ids = _ids;
            do
            {
                read = query.Fill(ids);
                for (int i = 0; i < read; i++)
                    identityReader.GetTermFor(ids[i]);
            }
            while (read != 0);
        }

        [Benchmark]
        public void RuntimeQueryOnlyIteration()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var indexSearcher = new IndexSearcher(Env, CreateFieldsMapping(bsc));

            var typeTerm = indexSearcher.TermQuery("Type", "Dog");
            var familyTerm = indexSearcher.TermQuery("Age", "15");
            var query = indexSearcher.And(typeTerm, familyTerm);

            Span<long> ids = _ids;
            while (query.Fill(ids) != 0);
        }

        [Benchmark]
        public void ParserQuery()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var indexSearcher = new IndexSearcher(Env, CreateFieldsMapping(bsc));

            var identityReader = indexSearcher.TermsReaderFor("id()");
            var evaluator = new CoraxQueryEvaluator(indexSearcher);
            var query = evaluator.Search(_queryDefinition.Query.Where);

            int read = 0;
            Span<long> ids = _ids;
            do
            {
                read = query.Fill(ids);
                for (int i = 0; i < read; i++)
                    identityReader.GetTermFor(ids[i]);
            }
            while (read != 0);
        }

        [Benchmark]
        public void ParserQueryOnlyIteration()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var indexSearcher = new IndexSearcher(Env, CreateFieldsMapping(bsc));
            var evaluator = new CoraxQueryEvaluator(indexSearcher);
            var query = evaluator.Search(_queryDefinition.Query.Where);

            Span<long> ids = _ids;
            while (query.Fill(ids) != 0);
        }
    }
}
