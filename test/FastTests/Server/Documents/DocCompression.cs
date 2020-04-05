using System;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents
{
    public class DocCompression : RavenTestBase
    {
        public DocCompression(ITestOutputHelper output) : base(output)
        {
        }

        public class User
        {
            public string Desc;
        }

        public static string RandomString(Random random, int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        [Fact]
        public void Can_write_many_documents_without_breakage()
        {
            var random = new Random(654);
            using var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.CompressedCollections.Add("Users")
            });

            var rnd = Enumerable.Range(1, 10)
                .Select(i => RandomString(random, 16))
                .ToList();
            using var s = store.OpenSession();
            for (int i = 0; i < 1024; i++)
            {
                s.Store(new User
                {
                    Desc = string.Join("-", Enumerable.Range(1, random.Next(1, 10)).Select(xi => rnd[xi]))
                }, "users/" + i);
            }
            s.SaveChanges();

        }

        [Fact]
        public void Can_update_many_documents_without_breakage()
        {
            var random = new Random(654);
            using var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.CompressedCollections.Add("Users")
            });

            var rnd = Enumerable.Range(1, 10)
                .Select(i => RandomString(random, 16))
                .ToList();

            using (var s = store.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    s.Store(new User
                    {
                        Desc = string.Join("-", Enumerable.Range(1, random.Next(1, 10)).Select(xi => rnd[xi]))
                    }, "users/" + i);
                }

                s.SaveChanges();
            }

            rnd = Enumerable.Range(1, 64)
                .Select(i => RandomString(random, 512))
                .ToList();

            using (var s = store.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    s.Store(new User
                    {
                        Desc = string.Join("-", Enumerable.Range(1, random.Next(1, 64)).Select(xi => rnd[xi]))
                    }, "users/" + i);
                }

                s.SaveChanges();
            }
        }

        [Fact]
        public void Can_update_many_documents_without_breakage_to_be_smaller()
        {
            var random = new Random(654);
            using var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.CompressedCollections.Add("Users")
            });

            var rnd = Enumerable.Range(1, 10)
                .Select(i => RandomString(random, 16))
                .ToList();

            using (var s = store.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    s.Store(new User
                    {
                        Desc = string.Join("-", Enumerable.Range(1, random.Next(1, 10)).Select(xi => rnd[xi]))
                    }, "users/" + i);
                }

                s.SaveChanges();
            }

            rnd = Enumerable.Range(1, 64)
                .Select(i => RandomString(random, 512))
                .ToList();

            using (var s = store.OpenSession())
            {
                for (int i = 256; i < 1024; i++)
                {
                    s.Store(new User
                    {
                        Desc = string.Join("-", Enumerable.Range(1, random.Next(1, 64)).Select(xi => rnd[xi]))
                    }, "users/" + i);
                }

                s.SaveChanges();
            }

            rnd = Enumerable.Range(1, 64)
                .Select(i => RandomString(random, 32))
                .ToList();

            using (var s = store.OpenSession())
            {
                for (int i = 128; i < 768; i++)
                {
                    s.Store(new User
                    {
                        Desc = string.Join("-", Enumerable.Range(1, random.Next(1, 64)).Select(xi => rnd[xi]))
                    }, "users/" + i);
                }

                for (int i = 0; i < 128; i++)
                {
                    s.Store(new User
                    {
                        Desc = string.Join("-", Enumerable.Range(1, random.Next(1, 64)).Select(xi => rnd[xi]))
                    }, "users/" + i+900);
                }

                s.SaveChanges();
            }
        }
    }
}
