using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.FileSystem.ClientApi
{
    public class UsingUntypedFilesQueryTests
    {
        private IAsyncFilesQuery<FileHeader> CreateUserQuery()
        {
            return new AsyncFilesQuery<FileHeader>(null, null);
        }

        [Fact]
        public void CanUnderstandSimpleEquality()
        {
            var q = CreateUserQuery().WhereEquals("Name", "file.test");

            Assert.Equal("__fileName:file.test", q.ToString());
        }

        [Fact]
        public void CanUnderstandSimpleEqualityWithVariable()
        {
            var file = "file.test" + 1;
            var q = CreateUserQuery().WhereEquals("Name", file);
            Assert.Equal("__fileName:file.test1", q.ToString());
        }

        [Fact]
        public void CanUnderstandSimpleContains()
        {
            var q = CreateUserQuery().WhereIn("Name", new[] { "file.test" });
            Assert.Equal("@in<__fileName>:(file.test)", q.ToString());
        }

        [Fact]
        public void CanUnderstandParamArrayContains()
        {
            var q = CreateUserQuery().WhereIn("Name", new[] { "file.csv", "file.txt" });
            Assert.Equal("@in<__fileName>:(file.csv , file.txt)", q.ToString());
        }

        [Fact]
        public void CanUnderstandArrayContains()
        {
            var array = new[] { "file.csv", "file.txt" };
            var q = CreateUserQuery().WhereIn("Name", array);
            Assert.Equal("@in<__fileName>:(file.csv , file.txt)", q.ToString());
        }

        [Fact]
        public void CanUnderstandArrayContainsWithOneElement()
        {
            var array = new[] { "file.csv" };
            var q = CreateUserQuery().WhereIn("Name", array);
            Assert.Equal("@in<__fileName>:(file.csv)", q.ToString());
        }

        [Fact]
        public void CanUnderstandArrayContainsWithZeroElements()
        {
            var array = new string[0];
            var q = CreateUserQuery().WhereIn("Name", array);
            Assert.Equal("@emptyIn<__fileName>:(no-results)", q.ToString());
        }

        [Fact]
        public void CanUnderstandEnumerableContains()
        {
            IEnumerable<string> list = new[] { "file.csv", "file.txt" };
            var q = CreateUserQuery().WhereIn("Name", list);
            Assert.Equal("@in<__fileName>:(file.csv , file.txt)", q.ToString());
        }

        [Fact]
        public void CanUnderstandSimpleContainsWithVariable()
        {
            var q = CreateUserQuery().WhereIn("Name", new[] { "file.test1" });
            Assert.Equal("@in<__fileName>:(file.test1)", q.ToString());
        }

        [Fact]
        public void NoOpShouldProduceEmptyString()
        {
            var q = CreateUserQuery();
            Assert.Equal("", q.ToString());
        }

        [Fact]
        public void CanUnderstandAnd()
        {
            var q = CreateUserQuery().WhereEquals("Name", "file.test")
                                     .AndAlso()
                                     .WhereEquals("Metadata-Item", "test");
            Assert.Equal("__fileName:file.test AND Metadata-Item:test", q.ToString());
        }

        [Fact]
        public void CanUnderstandOr()
        {
            var q = CreateUserQuery().WhereEquals("Name", "file.test")
                                     .OrElse()
                                     .WhereEquals("Metadata-Item", "test");
            Assert.Equal("__fileName:file.test OR Metadata-Item:test", q.ToString());
        }

        [Fact]
        public void CanUnderstandLessThan()
        {
            var q = CreateUserQuery().WhereLessThan("LastModified", new DateTime(2010, 05, 15));
            Assert.Equal("__modified:{* TO 2010-05-15T00:00:00.0000000}", q.ToString());
        }

        [Fact]
        public void CanUnderstandEqualOnDate()
        {
            var q = CreateUserQuery().WhereEquals("LastModified", new DateTime(2010, 05, 15));
            Assert.Equal("__modified:2010-05-15T00:00:00.0000000", q.ToString());
        }

        [Fact]
        public void CanUnderstandLessThanOrEqual()
        {
            var q = CreateUserQuery().WhereLessThanOrEqual("LastModified", new DateTime(2010, 05, 15));
            Assert.Equal("__modified:[* TO 2010-05-15T00:00:00.0000000]", q.ToString());
        }

        [Fact]
        public void CanUnderstandGreaterThan()
        {
            var q = CreateUserQuery().WhereGreaterThan("LastModified", new DateTime(2010, 05, 15));
            Assert.Equal("__modified:{2010-05-15T00:00:00.0000000 TO NULL}", q.ToString());
        }

        [Fact]
        public void CanUnderstandGreaterThanOrEqual()
        {
            var q = CreateUserQuery().WhereGreaterThanOrEqual("LastModified", new DateTime(2010, 05, 15));
            Assert.Equal("__modified:[2010-05-15T00:00:00.0000000 TO NULL]", q.ToString());
        }


        [Fact]
        public void CanUnderstandSimpleEqualityOnInt()
        {
            var q = CreateUserQuery().WhereEquals("TotalSize", 3);
            Assert.Equal("__size_numeric:[Ix3 TO Ix3]", q.ToString());
        }

        [Fact]
        public void CanUnderstandGreaterThanOnInt()
        {
            // should FilesQuery<T> understand how to generate range field names?
            var q = CreateUserQuery().WhereGreaterThan("TotalSize", 3);
            Assert.Equal("__size_numeric:{Ix3 TO NULL}", q.ToString());
        }
    }
}
