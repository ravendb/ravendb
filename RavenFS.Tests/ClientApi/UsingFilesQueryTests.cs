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
    public class UsingFilesQueryTests
    {
        [Fact]
        public void CanUnderstandSimpleEquality()
        {
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereEquals("Name", "file.test");

            Assert.Equal("Name:file.test", q.ToString());
        }

        [Fact]
        public void CanUnderstandSimpleEqualityWithVariable()
        {
            var file = "file.test" + 1;
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereEquals("Name", file);
            Assert.Equal("Name:file.test1", q.ToString());
        }

        [Fact]
        public void CanUnderstandSimpleContains()
        {
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereIn("Name", new[] { "ayende" });
            Assert.Equal("@in<Name>:(ayende)", q.ToString());
        }

        [Fact]
        public void CanUnderstandParamArrayContains()
        {
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereIn("Name", new[] { "ryan", "heath" });
            Assert.Equal("@in<Name>:(ryan,heath)", q.ToString());
        }

        [Fact]
        public void CanUnderstandArrayContains()
        {
            var array = new[] { "ryan", "heath" };
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereIn("Name", array);
            Assert.Equal("@in<Name>:(ryan,heath)", q.ToString());
        }

        [Fact]
        public void CanUnderstandArrayContainsWithPhrase()
        {
            var array = new[] { "ryan", "heath here" };
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereIn("Name", array);
            Assert.Equal("@in<Name>:(ryan,\"heath here\")", q.ToString());
        }

        [Fact]
        public void CanUnderstandArrayContainsWithOneElement()
        {
            var array = new[] { "ryan" };
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereIn("Name", array);
            Assert.Equal("@in<Name>:(ryan)", q.ToString());
        }

        [Fact]
        public void CanUnderstandArrayContainsWithZeroElements()
        {
            var array = new string[0];
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereIn("Name", array);
            Assert.Equal("@emptyIn<Name>:(no-results)", q.ToString());
        }

        [Fact]
        public void CanUnderstandEnumerableContains()
        {
            IEnumerable<string> list = new[] { "ryan", "heath" };
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereIn("Name", list);
            Assert.Equal("@in<Name>:(ryan,heath)", q.ToString());
        }

        [Fact]
        public void CanUnderstandSimpleContainsWithVariable()
        {
            var ayende = "ayende" + 1;
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereIn("Name", new[] { ayende });
            Assert.Equal("@in<Name>:(ayende1)", q.ToString());
        }

        [Fact]
        public void NoOpShouldProduceEmptyString()
        {
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>());
            Assert.Equal("", q.ToString());
        }

        [Fact]
        public void CanUnderstandAnd()
        {
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereEquals("Name", "ayende")
                .AndAlso()
                .WhereEquals("Email", "ayende@ayende.com");
            Assert.Equal("Name:ayende AND Email:ayende@ayende.com", q.ToString());
        }

        [Fact]
        public void CanUnderstandOr()
        {
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereEquals("Name", "ayende")
                .OrElse()
                .WhereEquals("Email", "ayende@ayende.com");
            Assert.Equal("Name:ayende OR Email:ayende@ayende.com", q.ToString());
        }

        [Fact]
        public void CanUnderstandLessThan()
        {
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereLessThan("Birthday", new DateTime(2010, 05, 15));
            Assert.Equal("Birthday:{* TO 2010-05-15T00:00:00.0000000}", q.ToString());
        }

        [Fact]
        public void CanUnderstandEqualOnDate()
        {
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereEquals("Birthday", new DateTime(2010, 05, 15));
            Assert.Equal("Birthday:2010-05-15T00:00:00.0000000", q.ToString());
        }

        [Fact]
        public void CanUnderstandLessThanOrEqual()
        {
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereLessThanOrEqual("Birthday", new DateTime(2010, 05, 15));
            Assert.Equal("Birthday:[* TO 2010-05-15T00:00:00.0000000]", q.ToString());
        }

        [Fact]
        public void CanUnderstandGreaterThan()
        {
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereGreaterThan("Birthday", new DateTime(2010, 05, 15));
            Assert.Equal("Birthday:{2010-05-15T00:00:00.0000000 TO NULL}", q.ToString());
        }

        [Fact]
        public void CanUnderstandGreaterThanOrEqual()
        {
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereGreaterThanOrEqual("Birthday", new DateTime(2010, 05, 15));
            Assert.Equal("Birthday:[2010-05-15T00:00:00.0000000 TO NULL]", q.ToString());
        }


        [Fact]
        public void CanUnderstandSimpleEqualityOnInt()
        {
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereEquals("Age", 3);
            Assert.Equal("Age:3", q.ToString());
        }

        [Fact]
        public void CanUnderstandGreaterThanOnInt()
        {
            // should FilesQuery<T> understand how to generate range field names?
            var q = ((IFilesQuery<FileHeader>)new FilesQuery<FileHeader>())
                .WhereGreaterThan("Age_Range", 3);
            Assert.Equal("Age_Range:{Ix3 TO NULL}", q.ToString());
        }
    }
}
