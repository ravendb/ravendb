using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace RavenFS.Tests.ClientApi
{
    public class UsingUntypedFilesQueryTests
    {
        private IFilesQuery<FileHeader> CreateUserQuery()
        {
            return new FilesQuery<FileHeader>(null, null);
        }

        [Fact]
        public void CanUnderstandSimpleEquality()
        {
            var q = CreateUserQuery().WhereEquals("Name", "file.test");

            Assert.Equal("Name:file.test", q.ToString());
        }

        [Fact]
        public void CanUnderstandSimpleEqualityWithVariable()
        {
            var file = "file.test" + 1;
            var q = CreateUserQuery().WhereEquals("Name", file);
            Assert.Equal("Name:file.test1", q.ToString());
        }

        [Fact]
        public void CanUnderstandSimpleContains()
        {
            var q = CreateUserQuery().WhereIn("Name", new[] { "file.test" });
            Assert.Equal("@in<Name>:(file.test)", q.ToString());
        }

        [Fact]
        public void CanUnderstandParamArrayContains()
        {
            var q = CreateUserQuery().WhereIn("Name", new[] { "file.csv", "file.txt" });
            Assert.Equal("@in<Name>:(file.csv,file.txt)", q.ToString());
        }

        [Fact]
        public void CanUnderstandArrayContains()
        {
            var array = new[] { "file.csv", "file.txt" };
            var q = CreateUserQuery().WhereIn("Name", array);
            Assert.Equal("@in<Name>:(file.csv,file.txt)", q.ToString());
        }

        [Fact]
        public void CanUnderstandArrayContainsWithOneElement()
        {
            var array = new[] { "file.csv" };
            var q = CreateUserQuery().WhereIn("Name", array);
            Assert.Equal("@in<Name>:(file.csv)", q.ToString());
        }

        [Fact]
        public void CanUnderstandArrayContainsWithZeroElements()
        {
            var array = new string[0];
            var q = CreateUserQuery().WhereIn("Name", array);
            Assert.Equal("@emptyIn<Name>:(no-results)", q.ToString());
        }

        [Fact]
        public void CanUnderstandEnumerableContains()
        {
            IEnumerable<string> list = new[] { "file.csv", "file.txt" };
            var q = CreateUserQuery().WhereIn("Name", list);
            Assert.Equal("@in<Name>:(file.csv,file.txt)", q.ToString());
        }

        [Fact]
        public void CanUnderstandSimpleContainsWithVariable()
        {
            var q = CreateUserQuery().WhereIn("Name", new[] { "file.test1" });
            Assert.Equal("@in<Name>:(file.test1)", q.ToString());
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
                                     .WhereEquals("Extension", "test");
            Assert.Equal("Name:file.test AND Extension:test", q.ToString());
        }

        [Fact]
        public void CanUnderstandOr()
        {
            var q = CreateUserQuery().WhereEquals("Name", "file.test")
                                     .OrElse()
                                     .WhereEquals("Extension", "test");
            Assert.Equal("Name:file.test OR Extension:test", q.ToString());
        }

        [Fact]
        public void CanUnderstandLessThan()
        {
            var q = CreateUserQuery().WhereLessThan("CreationDate", new DateTime(2010, 05, 15));
            Assert.Equal("CreationDate:{* TO 2010-05-15T00:00:00.0000000}", q.ToString());
        }

        [Fact]
        public void CanUnderstandEqualOnDate()
        {
            var q = CreateUserQuery().WhereEquals("CreationDate", new DateTime(2010, 05, 15));
            Assert.Equal("CreationDate:2010-05-15T00:00:00.0000000", q.ToString());
        }

        [Fact]
        public void CanUnderstandLessThanOrEqual()
        {
            var q = CreateUserQuery().WhereLessThanOrEqual("CreationDate", new DateTime(2010, 05, 15));
            Assert.Equal("CreationDate:[* TO 2010-05-15T00:00:00.0000000]", q.ToString());
        }

        [Fact]
        public void CanUnderstandGreaterThan()
        {
            var q = CreateUserQuery().WhereGreaterThan("CreationDate", new DateTime(2010, 05, 15));
            Assert.Equal("CreationDate:{2010-05-15T00:00:00.0000000 TO NULL}", q.ToString());
        }

        [Fact]
        public void CanUnderstandGreaterThanOrEqual()
        {
            var q = CreateUserQuery().WhereGreaterThanOrEqual("CreationDate", new DateTime(2010, 05, 15));
            Assert.Equal("CreationDate:[2010-05-15T00:00:00.0000000 TO NULL]", q.ToString());
        }


        [Fact]
        public void CanUnderstandSimpleEqualityOnInt()
        {
            var q = CreateUserQuery().WhereEquals("TotalSize", 3);
            Assert.Equal("TotalSize:3", q.ToString());
        }

        [Fact]
        public void CanUnderstandGreaterThanOnInt()
        {
            // should FilesQuery<T> understand how to generate range field names?
            var q = CreateUserQuery().WhereGreaterThan("TotalSize_Range", 3);
            Assert.Equal("TotalSize_Range:{Ix3 TO NULL}", q.ToString());
        }
    }
}
