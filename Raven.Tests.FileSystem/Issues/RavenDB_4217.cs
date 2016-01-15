// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4217.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_4217 : RavenFilesTestBase
    {
        [Fact]
        public async Task metadata_keys_are_consistently_upper_cased_on_first_letters_on_purpose()
        {
            using (var store = NewStore())
            {
                await store.AsyncFilesCommands.UploadAsync("f1", CreateRandomFileStream(2), new RavenJObject()
                {
                    {"test", "1"},
                    {"testKey", "2"},
                    {"test-key", "3"},
                });

                var terms = await store.AsyncFilesCommands.GetSearchFieldsAsync();

                Assert.Contains("Test", terms);
                Assert.DoesNotContain("test", terms);

                Assert.Contains("TestKey", terms);
                Assert.DoesNotContain("testKey", terms);

                Assert.Contains("Test-Key", terms);
                Assert.DoesNotContain("test-key", terms);
            }
        }

        [Fact]
        public async Task searching_is_case_sensitive_on_metadata_keys()
        {
            using (var store = NewStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("file", CreateRandomFileStream(4), new RavenJObject()
                    {
                        { "Property", "ValUe" }
                    });

                    await session.SaveChangesAsync();

                    Assert.Equal(1, (await session.Query().WhereEquals("Property", "value").ToListAsync()).Count);
                    Assert.Equal(0, (await session.Query().WhereEquals("property", "ValUe").ToListAsync()).Count);
                }
            }
        }

        [Fact]
        public async Task searching_by_numeric_field_should_be_case_sensitive_on_metadata_key()
        {
            using (var store = NewStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("file", CreateRandomFileStream(4), new RavenJObject()
                    {
                        { "Number", 10 }
                    });

                    await session.SaveChangesAsync();

                    Assert.Equal(1, (await session.Query().WhereEquals("Number", 10).ToListAsync()).Count);
                    Assert.Equal(0, (await session.Query().WhereEquals("number", 10).ToListAsync()).Count);

                    Assert.Equal(1, (await session.Query().WhereBetween("Number", 9, 11).ToListAsync()).Count);
                    Assert.Equal(0, (await session.Query().WhereBetween("number", 9, 11).ToListAsync()).Count);
                }
            }
        }
    }
}