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
    public class UsingStronglyTypedFilesQueryTests
    {
        private IFilesQuery<FileHeader> CreateUserQuery()
        {
            return new FilesQuery<FileHeader>(null, null);
        }

        [Fact]
        public void WhereEqualsSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().WhereEquals("Name", "file.test").ToString(),
                         CreateUserQuery().WhereEquals(x => x.Name, "file.test").ToString());
            Assert.Equal(CreateUserQuery().WhereEquals("Name", "file.test").ToString(), 
                         CreateUserQuery().WhereEquals(x => x.Name, "file.test").ToString());
        }

        [Fact]
        public void WhereInSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().WhereIn("Name", new[] { "file.test", "another.csv" }).ToString(),
                         CreateUserQuery().WhereIn(x => x.Name, new[] { "file.test", "another.csv" }).ToString());
        }

        [Fact]
        public void WhereStartsWithSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().WhereStartsWith("Name", "file").ToString(),
                         CreateUserQuery().WhereStartsWith(x => x.Name, "file").ToString());
        }

        [Fact]
        public void WhereEndsWithSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().WhereEndsWith("Name", "file").ToString(),
                         CreateUserQuery().WhereEndsWith(x => x.Name, "file").ToString());
        }

        [Fact]
        public void WhereBetweenSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().WhereBetween("Name", "file", "zaphod").ToString(),
                CreateUserQuery().WhereBetween(x => x.Name, "file", "zaphod").ToString());
        }

        [Fact]
        public void WhereBetweenOrEqualSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().WhereBetweenOrEqual("Name", "file", "zaphod").ToString(),
                CreateUserQuery().WhereBetweenOrEqual(x => x.Name, "file", "zaphod").ToString());
        }

        [Fact]
        public void WhereGreaterThanSameAsUntypedCounterpart()
        {
            var value = DateTimeOffset.UtcNow;

            Assert.Equal(CreateUserQuery().WhereGreaterThan("CreationDate", value).ToString(),
                         CreateUserQuery().WhereGreaterThan(x => x.CreationDate, value).ToString());
        }

        [Fact]
        public void WhereGreaterThanOrEqualSameAsUntypedCounterpart()
        {
            var value = DateTimeOffset.UtcNow;

            Assert.Equal(CreateUserQuery().WhereGreaterThanOrEqual("CreationDate", value).ToString(),
                         CreateUserQuery().WhereGreaterThanOrEqual(x => x.CreationDate, value).ToString());
        }

        [Fact]
        public void WhereLessThanSameAsUntypedCounterpart()
        {
            var value = DateTimeOffset.UtcNow;

            Assert.Equal(CreateUserQuery().WhereLessThan("CreationDate", value).ToString(),
                         CreateUserQuery().WhereLessThan(x => x.CreationDate, value).ToString());
        }

        [Fact]
        public void WhereLessThanOrEqualSameAsUntypedCounterpart()
        {
            var value = DateTimeOffset.UtcNow;

            Assert.Equal(CreateUserQuery().WhereLessThanOrEqual("CreationDate", value).ToString(),
                         CreateUserQuery().WhereLessThanOrEqual(x => x.CreationDate, value).ToString());
        }

    }
}
