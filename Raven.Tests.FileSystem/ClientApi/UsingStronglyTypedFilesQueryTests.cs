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
    public class UsingStronglyTypedFilesQueryTests
    {
        private IAsyncFilesQuery<FileHeader> CreateUserQuery()
        {
            return new AsyncFilesQuery<FileHeader>(null, null);
        }

        [Fact]
        public void WhereEqualsSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().WhereEquals("__fileName", "file.test").ToString(),
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

            Assert.Equal(CreateUserQuery().WhereGreaterThan("LastModified", value).ToString(),
                         CreateUserQuery().WhereGreaterThan(x => x.LastModified, value).ToString());
        }

        [Fact]
        public void WhereGreaterThanOrEqualSameAsUntypedCounterpart()
        {
            var value = DateTimeOffset.UtcNow;

            Assert.Equal(CreateUserQuery().WhereGreaterThanOrEqual("LastModified", value).ToString(),
                         CreateUserQuery().WhereGreaterThanOrEqual(x => x.LastModified, value).ToString());
        }

        [Fact]
        public void WhereLessThanSameAsUntypedCounterpart()
        {
            var value = DateTimeOffset.UtcNow;

            Assert.Equal(CreateUserQuery().WhereLessThan("LastModified", value).ToString(),
                         CreateUserQuery().WhereLessThan(x => x.LastModified, value).ToString());
        }

        [Fact]
        public void WhereLessThanOrEqualSameAsUntypedCounterpart()
        {
            var value = DateTimeOffset.UtcNow;

            Assert.Equal(CreateUserQuery().WhereLessThanOrEqual("LastModified", value).ToString(),
                         CreateUserQuery().WhereLessThanOrEqual(x => x.LastModified, value).ToString());
        }

        [Fact]
        public void WhereEqualsOnMetadataKey ()
        {
            Assert.Equal(CreateUserQuery().WhereEquals("Kiss", "true").ToString(),
                         CreateUserQuery().WhereEquals(x => x.Metadata["Kiss"], "true").ToString());
        }

        [Fact]
        public void WhereEqualsOnMetadataOnKnownKey()
        {
            Assert.Equal(CreateUserQuery().WhereEquals("__fileName", "file.test").ToString(),
                         CreateUserQuery().WhereEquals(x => x.Metadata["Name"], "file.test").ToString());
        }

    }
}
