// -----------------------------------------------------------------------
//  <copyright file="RavenDB-13100.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13100 : NoDisposalNeeded 
    {
        public RavenDB_13100(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanDetectTimeSeriesIndexSourceMethodSyntax()
        {
            string map = "timeSeries.Companies.SelectMany(ts => ts.Entries, (ts, entry) => new {" +
                         "   HeartBeat = entry.Values[0], " +
                         "   Date = entry.Timestamp.Date, " +
                         "   User = ts.DocumentId " +
                         "});";
            
            Assert.Equal(IndexSourceType.TimeSeries, IndexDefinitionHelper.DetectStaticIndexSourceType(map));
        }

        [Fact]
        public void CanDetectDocumentsIndexSourceMethodSyntax()
        {
            string map = "docs.Users.OrderBy(user => user.Id).Select(user => new { user.Name })";
            Assert.Equal(IndexSourceType.Documents, IndexDefinitionHelper.DetectStaticIndexSourceType(map));
        }

        [Fact]
        public void CanDetectTimeSeriesIndexSourceLinqSyntaxAllTs()
        {
            string map = "from ts in timeSeries";
            Assert.Equal(IndexSourceType.TimeSeries, IndexDefinitionHelper.DetectStaticIndexSourceType(map));
        }
        
        [Fact]
        public void CanDetectTimeSeriesIndexSourceLinqSyntaxSingleTs()
        {
            string map = "from ts in timeSeries.Users";
            Assert.Equal(IndexSourceType.TimeSeries, IndexDefinitionHelper.DetectStaticIndexSourceType(map));
        }
        
        [Fact]
        public void CanDetectTimeSeriesIndexSourceLinqSyntaxCanStripWhiteSpace()
        {
            string map = "\t\t  \t from    ts  \t \t in  \t \t timeSeries.Users";
            Assert.Equal(IndexSourceType.TimeSeries, IndexDefinitionHelper.DetectStaticIndexSourceType(map));
        }
    }
}
