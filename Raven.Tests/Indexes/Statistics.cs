using System;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Indexes
{
    public class Statistics : AbstractDocumentStorageTest, IDisposable
    {
        private readonly DocumentDatabase db;

        public Statistics()
        {
            db = new DocumentDatabase(new RavenConfiguration { DataDirectory = "raven.db.test.esent" });
            db.SpinBackgroundWorkers();

            db.PutIndex("pagesByTitle2",
                        @"
                    from doc in docs
                    where doc.type == ""page""
                    select new {  f = 2 / doc.size };
                ");
        }

        #region IDisposable Members

        public void Dispose()
        {
            db.Dispose();
        }

        #endregion

        [Fact]
        public void Can_get_stats_for_indexing_without_any_indexing()
        {
            Assert.Equal(1, db.Statistics.Indexes.Length);
            Assert.Equal("pagesByTitle2", db.Statistics.Indexes[0].Name);
            Assert.Equal(0, db.Statistics.Indexes[0].IndexingAttempts);
        }

        [Fact]
        public void Can_get_stats_for_indexing()
        {
            db.Put("1", Guid.Empty,
                   JObject.Parse(
                       @"{
                type: 'page', 
                some: 'val', 
                other: 'var', 
                content: 'this is the content', 
                title: 'hello world', 
                size: 1,
                '@metadata': {'@id': 1}
            }"),
                   new JObject());

            QueryResult docs;
            do
            {
                docs = db.Query("pagesByTitle2", "some:val", 0, 10);
                if (docs.IsStale)
                    Thread.Sleep(100);
            } while (docs.IsStale);

            Assert.Equal("pagesByTitle2", db.Statistics.Indexes[0].Name);
            Assert.Equal(1, db.Statistics.Indexes[0].IndexingAttempts);
            Assert.Equal(1, db.Statistics.Indexes[0].IndexingSuccesses);
        }

        [Fact]
        public void Can_get_stats_for_indexing_including_errors()
        {
            db.Put("1", Guid.Empty,
                   JObject.Parse(
                       @"{
                type: 'page', 
                some: 'val', 
                other: 'var', 
                content: 'this is the content', 
                title: 'hello world', 
                size: 0,
                '@metadata': {'@id': 1}
            }"),
                   new JObject());
            db.Put("2", Guid.Empty,
                   JObject.Parse(
                       @"{
                type: 'page', 
                some: 'val', 
                other: 'var', 
                content: 'this is the content', 
                title: 'hello world', 
                size: 1,
                '@metadata': {'@id': 1}
            }"),
                   new JObject());

            QueryResult docs;
            do
            {
                docs = db.Query("pagesByTitle2", "some:val", 0, 10);
                if (docs.IsStale)
                    Thread.Sleep(100);
            } while (docs.IsStale);

            Assert.Equal("pagesByTitle2", db.Statistics.Indexes[0].Name);
            Assert.Equal(2, db.Statistics.Indexes[0].IndexingAttempts);
            Assert.Equal(1, db.Statistics.Indexes[0].IndexingErrors);
            Assert.Equal(1, db.Statistics.Indexes[0].IndexingSuccesses);
        }

        [Fact]
        public void Can_get_details_about_indexing_errors()
        {
            db.Put("1", Guid.Empty,
                   JObject.Parse(
                       @"{
                type: 'page', 
                some: 'val', 
                other: 'var', 
                content: 'this is the content', 
                title: 'hello world', 
                size: 0,
                '@metadata': {'@id': 1}
            }"),
                   new JObject());

            QueryResult docs;
            do
            {
                docs = db.Query("pagesByTitle2", "some:val", 0, 10);
                if (docs.IsStale)
                    Thread.Sleep(100);
            } while (docs.IsStale);

            Assert.Equal("1", db.Statistics.Errors[0].Document);
            Assert.Equal("pagesByTitle2", db.Statistics.Errors[0].Index);
            Assert.Contains("Attempted to divide by zero.", db.Statistics.Errors[0].Error);
        }
    }
}