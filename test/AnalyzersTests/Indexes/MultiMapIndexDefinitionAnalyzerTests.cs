using System.Collections.Immutable;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Raven.Analyzers.Indexes;
using AnalyzersTests.Framework;
using Raven.Analyzers;
using Xunit;

namespace AnalyzersTests.Indexes
{
    public class MultiMapIndexDefinitionAnalyzerTests
    {
        private const string CommonUsings = @"
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Indexes.Counters;
";

        [Fact]
        public async Task MultiMap_Two_AddMap_Calls_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyMultiMapIndex : AbstractMultiMapIndexCreationTask<MyResult>
{
    public MyMultiMapIndex()
    {
        AddMap<Company>(companies => from c in companies select new MyResult { Name = c.Name });
        AddMap<Employee>(employees => from e in employees select new MyResult { Name = e.FirstName });
    }
}

class MyResult { public string Name { get; set; } }
class Company { public string Name { get; set; } }
class Employee { public string FirstName { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task MultiMap_No_AddMap_In_Ctor_Reports_Warning()
        {
            const string source = CommonUsings + @"
class MyMultiMapIndex : AbstractMultiMapIndexCreationTask<MyResult>
{
    public MyMultiMapIndex() { }
}

class MyResult { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.MultiMapIndexMissingAddMap, d.Id);
            Assert.Equal(DiagnosticSeverity.Warning, d.Severity);
            Assert.Contains("MyMultiMapIndex", d.GetMessage());
        }

        [Fact]
        public async Task MultiMap_Single_AddMap_Suggests_Regular_Index()
        {
            const string source = CommonUsings + @"
class MyMultiMapIndex : AbstractMultiMapIndexCreationTask<MyResult>
{
    public MyMultiMapIndex()
    {
        AddMap<Company>(companies => from c in companies select new MyResult { Name = c.Name });
    }
}

class MyResult { public string Name { get; set; } }
class Company { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.MultiMapIndexSingleAddMap, d.Id);
            Assert.Equal(DiagnosticSeverity.Info, d.Severity);
            Assert.Contains("MyMultiMapIndex", d.GetMessage());
        }

        [Fact]
        public async Task MultiMap_AddMap_Only_In_Helper_Method_Reports_Warning()
        {
            const string source = CommonUsings + @"
class MyMultiMapIndex : AbstractMultiMapIndexCreationTask<MyResult>
{
    public MyMultiMapIndex()
    {
        Configure();
    }

    private void Configure()
    {
        AddMap<Company>(companies => from c in companies select new MyResult { Name = c.Name });
        AddMap<Employee>(employees => from e in employees select new MyResult { Name = e.FirstName });
    }
}

class MyResult { public string Name { get; set; } }
class Company { public string Name { get; set; } }
class Employee { public string FirstName { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.MultiMapIndexMissingAddMap, d.Id);
        }

        [Fact]
        public async Task MultiMap_AddMap_And_AddMapForAll_Counts_As_Two_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyMultiMapIndex : AbstractMultiMapIndexCreationTask<MyResult>
{
    public MyMultiMapIndex()
    {
        AddMap<Company>(companies => from c in companies select new MyResult { Name = c.Name });
        AddMapForAll<Employee>(employees => from e in employees select new MyResult { Name = e.FirstName });
    }
}

class MyResult { public string Name { get; set; } }
class Company { public string Name { get; set; } }
class Employee { public string FirstName { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task MultiMapTimeSeries_Two_AddMap_Calls_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyTsIndex : AbstractMultiMapTimeSeriesIndexCreationTask<MyResult>
{
    public MyTsIndex()
    {
        AddMap<Company>(""Heartrate"", segments => from s in segments select new MyResult { Name = ""company"" });
        AddMap<Employee>(""Heartrate"", segments => from s in segments select new MyResult { Name = ""employee"" });
    }
}

class MyResult { public string Name { get; set; } }
class Company { public string Name { get; set; } }
class Employee { public string FirstName { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task MultiMapTimeSeries_No_AddMap_In_Ctor_Reports_Warning()
        {
            const string source = CommonUsings + @"
class MyTsIndex : AbstractMultiMapTimeSeriesIndexCreationTask<MyResult>
{
    public MyTsIndex() { }
}

class MyResult { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.MultiMapIndexMissingAddMap, d.Id);
            Assert.Equal(DiagnosticSeverity.Warning, d.Severity);
        }

        [Fact]
        public async Task MultiMapTimeSeries_Single_AddMap_Suggests_Regular_Index()
        {
            const string source = CommonUsings + @"
class MyTsIndex : AbstractMultiMapTimeSeriesIndexCreationTask<MyResult>
{
    public MyTsIndex()
    {
        AddMap<Company>(""Heartrate"", segments => from s in segments select new MyResult { Name = ""company"" });
    }
}

class MyResult { public string Name { get; set; } }
class Company { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.MultiMapIndexSingleAddMap, d.Id);
            Assert.Equal(DiagnosticSeverity.Info, d.Severity);
        }

        [Fact]
        public async Task MultiMapCounters_Two_AddMap_Calls_No_Diagnostic()
        {
            const string source = CommonUsings + @"
class MyCountersIndex : AbstractMultiMapCountersIndexCreationTask<MyResult>
{
    public MyCountersIndex()
    {
        AddMap<Company>(""Likes"", counters => from c in counters select new MyResult { Name = ""company"" });
        AddMap<Employee>(""Likes"", counters => from c in counters select new MyResult { Name = ""employee"" });
    }
}

class MyResult { public string Name { get; set; } }
class Company { public string Name { get; set; } }
class Employee { public string FirstName { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Assert.Empty(diagnostics);
        }

        [Fact]
        public async Task MultiMapCounters_No_AddMap_In_Ctor_Reports_Warning()
        {
            const string source = CommonUsings + @"
class MyCountersIndex : AbstractMultiMapCountersIndexCreationTask<MyResult>
{
    public MyCountersIndex() { }
}

class MyResult { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.MultiMapIndexMissingAddMap, d.Id);
            Assert.Equal(DiagnosticSeverity.Warning, d.Severity);
        }

        [Fact]
        public async Task MultiMapCounters_Single_AddMap_Suggests_Regular_Index()
        {
            const string source = CommonUsings + @"
class MyCountersIndex : AbstractMultiMapCountersIndexCreationTask<MyResult>
{
    public MyCountersIndex()
    {
        AddMap<Company>(""Likes"", counters => from c in counters select new MyResult { Name = ""company"" });
    }
}

class MyResult { public string Name { get; set; } }
class Company { public string Name { get; set; } }
";
            ImmutableArray<Diagnostic> diagnostics =
                await RavenAnalyzerTest.AnalyzeAsync<IndexDefinitionAnalyzer>(source);

            Diagnostic d = Assert.Single(diagnostics);
            Assert.Equal(DiagnosticIds.MultiMapIndexSingleAddMap, d.Id);
            Assert.Equal(DiagnosticSeverity.Info, d.Severity);
        }
    }
}
