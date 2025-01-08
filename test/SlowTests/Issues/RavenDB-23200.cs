using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using SlowTests.Core.Utils.Entities;
using SlowTests.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_23200 : RavenTestBase
    {
        public RavenDB_23200(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [InlineDataWithRandomSeed]
        public void CanSortRandomAscii(int seed)
        {
            using (var store = GetDocumentStore())
            {
                new UserIndex().Execute(store);

                var uniqueStrings = new HashSet<string>();
                var random = new Random(Seed: seed);

                const int asciiStart = 33;
                const int asciiEnd = 126;

                while (uniqueStrings.Count < 16 * 1024) 
                {
                    var length = random.Next(1, 101);

                    var chars = new char[length];
                    for (var j = 0; j < length; j++)
                    {
                        chars[j] = (char)random.Next(asciiStart, asciiEnd + 1);
                    }

                    uniqueStrings.Add(new string(chars));
                }

                var sortedStrings = new List<string>(uniqueStrings);
                sortedStrings.Sort(string.CompareOrdinal);

                using (var bulk = store.BulkInsert())
                {
                    foreach (var str in uniqueStrings)
                    {
                        bulk.Store(new User { Name = str });
                    }
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<User, UserIndex>()
                        .OrderBy(x => x.Name).Select(x => x.Name).ToList();

                    for (var i = 0; i < sortedStrings.Count; i++)
                    {
                        Assert.Equal(sortedStrings[i], results[i]);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [InlineDataWithRandomSeed]
        public void CanSortRandomNonAscii(int seed)
        {
            using (var store = GetDocumentStore())
            {
                new UserIndex().Execute(store);

                var uniqueStrings = new HashSet<string>();
                var random = new Random(Seed: seed);

                const int hebrewStart = 0x0590; // Hebrew block
                const int hebrewEnd = 0x05FF;
                const int polishStart = 0x0100; // Extended Latin block for Polish characters
                const int polishEnd = 0x017F;
                const int cyrillicStart = 0x0400; // Cyrillic block
                const int cyrillicEnd = 0x04FF;
                const int greekStart = 0x0370; // Greek block
                const int greekEnd = 0x03FF;
                const int chineseStart = 0x4E00; // Chinese Simplified block
                const int chineseEnd = 0x9FFF;
                const int japaneseHiraganaStart = 0x3040; // Hiragana block
                const int japaneseHiraganaEnd = 0x309F;
                const int japaneseKatakanaStart = 0x30A0; // Katakana block
                const int japaneseKatakanaEnd = 0x30FF;
                const int japaneseKanjiStart = 0x4E00; // Kanji block (same as Chinese)
                const int japaneseKanjiEnd = 0x9FFF;
                const int koreanStart = 0xAC00; // Hangul block
                const int koreanEnd = 0xD7AF;
                const int spanishStart = 0x00C0; // Extended Latin block for Spanish characters
                const int spanishEnd = 0x00FF;
                const int smileyStart = 0x1F600; // Emoticons block
                const int smileyEnd = 0x1F64F;

                var allLanguagesRanges = new (int start, int end)[]
                {
                    (hebrewStart, hebrewEnd),
                    (polishStart, polishEnd),
                    (cyrillicStart, cyrillicEnd),
                    (greekStart, greekEnd),
                    (chineseStart, chineseEnd),
                    (japaneseHiraganaStart, japaneseHiraganaEnd),
                    (japaneseKatakanaStart, japaneseKatakanaEnd),
                    (japaneseKanjiStart, japaneseKanjiEnd),
                    (koreanStart, koreanEnd),
                    (spanishStart, spanishEnd),
                    (smileyStart, smileyEnd)
                };

                while (uniqueStrings.Count < 32 * 1024)
                {
                    var length = random.Next(1, 101);

                    var chars = new char[length];
                    for (var j = 0; j < length; j++)
                    {
                        var languageRange = allLanguagesRanges[random.Next(allLanguagesRanges.Length)];
                        chars[j] = (char)random.Next(languageRange.start, languageRange.end + 1);
                    }

                    uniqueStrings.Add(new string(chars));
                }

                var sortedStrings = new List<string>(uniqueStrings);
                sortedStrings.Sort(string.CompareOrdinal);

                using (var bulk = store.BulkInsert())
                {
                    foreach (var str in uniqueStrings)
                    {
                        bulk.Store(new User { Name = str });
                    }
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<User, UserIndex>()
                        .OrderBy(x => x.Name).Select(x => x.Name).ToList();

                    for (var i = 0; i < sortedStrings.Count; i++)
                    {
                        Assert.Equal(sortedStrings[i], results[i]);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [InlineDataWithRandomSeed]
        public void CanSortRandomMixAsciiAndNonAscii(int seed)
        {
            using (var store = GetDocumentStore())
            {
                new UserIndex().Execute(store);

                var uniqueStrings = new HashSet<string>();
                var random = new Random(Seed: seed);

                const int asciiStart = 33;
                const int asciiEnd = 126;
                const int hebrewStart = 0x0590; // Hebrew block
                const int hebrewEnd = 0x05FF;
                const int polishStart = 0x0100; // Extended Latin block for Polish characters
                const int polishEnd = 0x017F;
                const int cyrillicStart = 0x0400; // Cyrillic block
                const int cyrillicEnd = 0x04FF;
                const int greekStart = 0x0370; // Greek block
                const int greekEnd = 0x03FF;
                const int chineseStart = 0x4E00; // Chinese Simplified block
                const int chineseEnd = 0x9FFF;
                const int japaneseHiraganaStart = 0x3040; // Hiragana block
                const int japaneseHiraganaEnd = 0x309F;
                const int japaneseKatakanaStart = 0x30A0; // Katakana block
                const int japaneseKatakanaEnd = 0x30FF;
                const int japaneseKanjiStart = 0x4E00; // Kanji block (same as Chinese)
                const int japaneseKanjiEnd = 0x9FFF;
                const int koreanStart = 0xAC00; // Hangul block
                const int koreanEnd = 0xD7AF;
                const int spanishStart = 0x00C0; // Extended Latin block for Spanish characters
                const int spanishEnd = 0x00FF;
                const int smileyStart = 0x1F600; // Emoticons block
                const int smileyEnd = 0x1F64F;

                var allLanguagesRanges = new (int start, int end)[]
                {
                    (hebrewStart, hebrewEnd),
                    (polishStart, polishEnd),
                    (cyrillicStart, cyrillicEnd),
                    (greekStart, greekEnd),
                    (chineseStart, chineseEnd),
                    (japaneseHiraganaStart, japaneseHiraganaEnd),
                    (japaneseKatakanaStart, japaneseKatakanaEnd),
                    (japaneseKanjiStart, japaneseKanjiEnd),
                    (koreanStart, koreanEnd),
                    (spanishStart, spanishEnd),
                    (smileyStart, smileyEnd)
                };

                while (uniqueStrings.Count < 32 * 1024)
                {
                    var type = random.Next(3); // 0 = ASCII only, 1 = non-ASCII only, 2 = mixed

                    var length = random.Next(10, 11);
                    var chars = new char[length];

                    for (var j = 0; j < length; j++)
                    {
                        switch (type)
                        {
                            case 0:
                                // ascii only
                                chars[j] = (char)random.Next(asciiStart, asciiEnd + 1);
                                break;
                            case 1:
                                // non-ascii only
                                var languageRange = allLanguagesRanges[random.Next(allLanguagesRanges.Length)];
                                chars[j] = (char)random.Next(languageRange.start, languageRange.end + 1);
                                break;
                            default:
                            {
                                // randomly pick ascii or non-ascii for this character
                                if (random.Next(2) == 0)
                                {
                                    chars[j] = (char)random.Next(asciiStart, asciiEnd + 1);
                                }
                                else
                                {
                                    var range = allLanguagesRanges[random.Next(allLanguagesRanges.Length)];
                                    chars[j] = (char)random.Next(range.start, range.end + 1);
                                }

                                break;
                            }
                        }
                    }

                    uniqueStrings.Add(new string(chars));
                }

                var sortedStrings = new List<string>(uniqueStrings);
                sortedStrings.Sort(string.CompareOrdinal);

                using (var bulk = store.BulkInsert())
                {
                    foreach (var str in uniqueStrings)
                    {
                        bulk.Store(new User { Name = str });
                    }
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<User, UserIndex>()
                        .OrderBy(x => x.Name).Select(x => x.Name).ToList();

                    for (var i = 0; i < sortedStrings.Count; i++)
                    {
                        Assert.Equal(sortedStrings[i], results[i]);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanSortNonAscii(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new UserIndex().Execute(store);

                const string str1 = "\U000ad10eª󹬐Uư䵙􆒏\U000aa7c6\U0006237d";
                const string str2 = "�焗�";

                var userList = new List<User>();
                userList.Add(new User { Name = str1 });
                userList.Add(new User { Name = str2 });

                using (var session = store.OpenSession())
                {
                    session.Store(userList[0]);
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(userList[1]);
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var orderedUserList = userList.OrderBy(user => user.Name).ToList();

                    var results = session.Query<User, UserIndex>().OrderBy(x => x.Name).ToList();

                    Assert.Equal(orderedUserList[0].Name, results[0].Name);
                    Assert.Equal(orderedUserList[1].Name, results[1].Name);
                }
            }
        }

        private class UserIndex : AbstractIndexCreationTask<User>
        {
            public UserIndex()
            {
                Map = users => from user in users
                    select new
                    {
                        user.Name
                    };

                Analyzers.Add(c => c.Name, "WhitespaceAnalyzer");
            }
        }
    }
}
