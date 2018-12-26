using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Security.Policy;
using FastTests;
using Lextm.SharpSnmpLib;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Extensions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12087 : RavenTestBase
    {
        [Fact]
        public void ShouldMapIndexWithSubstring()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(_user);
                    session.SaveChanges();
                    
                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Maps = {
                            @"from user in docs.Users 
                                select new {
                                            SubstringOfMyName = user.MyName.Substring(0),
                                            SubstringOfMyNameWithLength = user.MyName.Substring(1, 2),
                                            SubstringOfMyTimeSpan = user.MyTimeSpan.Substring(0),
                                            SubstringOfMyTimeSpanWithLength = user.MyTimeSpan.Substring(1, 2),
                                            SubstringOfMyDateTime = user.MyDateTime.Substring(0),
                                            SubstringOfMyDateTimeWithLength = user.MyDateTime.Substring(1, 2),
                                            SubstringOfMyDateTimeOffset = user.MyDateTimeOffset.Substring(0),
                                            SubstringOfMyDateTimeOffsetWithLength = user.MyDateTimeOffset.Substring(1, 2),

                                            SubstringOfMyUser = user.MyUser.Substring(0),
                                            SubstringOfMyUserWithLength = user.MyUser.Substring(1, 2),

                                            SubstringOfMyInt = user.MyInt.Substring(0),
                                            SubstringOfMyIntWithLength = user.MyInt.Substring(1, 2),
                                            SubstringOfMyLong = user.MyLong.Substring(0),
                                            SubstringOfMyLongWithLength = user.MyLong.Substring(1, 2),
                                            SubstringOfMyDouble = user.MyDouble.Substring(0),
                                            SubstringOfMyDoubleWithLength = user.MyDouble.Substring(1, 2),
                                            SubstringOfMyFloat = user.MyFloat.Substring(0),
                                            SubstringOfMyFloatWithLength = user.MyFloat.Substring(1, 2),
                                            }"
                        },
                        Type = IndexType.Map,
                        Name = "TestIndexSubstring"
                    }));
                    WaitForIndexing(store);

                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyName == "Egor").OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyNameWithLength == "go").OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyTimeSpan == _timeSpan.ToString().Substring(0)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyTimeSpanWithLength == _timeSpan.ToString().Substring(1, 2)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyDateTime == _dateTime.ToString(CultureInfo.InvariantCulture).Substring(0)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyDateTimeWithLength == _dateTime.ToString(CultureInfo.InvariantCulture).Substring(1, 2)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyDateTimeOffset == _dateTimeOffset.ToString(CultureInfo.InvariantCulture).Substring(0)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyDateTimeOffsetWithLength == _dateTimeOffset.ToString(CultureInfo.InvariantCulture).Substring(1, 2)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyUser == "{\"name\":\"egor2\",\"number\":322}").OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyUserWithLength == "\"n").OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyInt == _user.MyInt.ToString().Substring(0)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyIntWithLength == _user.MyInt.ToString().Substring(1, 2)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyLong == _user.MyLong.ToString().Substring(0)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyLongWithLength == _user.MyLong.ToString().Substring(1, 2)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyFloat == "4.1999998092651367").OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyFloatWithLength == ".1").OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyDouble == _user.MyDouble.ToString(CultureInfo.InvariantCulture).Substring(0)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexSubstring").Where(x => x.SubstringOfMyDoubleWithLength == _user.MyDouble.ToString(CultureInfo.InvariantCulture).Substring(1, 2)).OfType<User>().ToArray().Length);
                }
            }
        }

        [Fact]
        public void ShouldThrowOnMapIndexWithSubstring()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(_user);
                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = {
                        @"from user in docs.Users 
                            select new { SubstringOfMyArray = user.MyArray.Substring(0) }"
                    },
                    Type = IndexType.Map,
                    Name = "TestIndexSubstringOfArray1"
                }));
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = {
                        @"from user in docs.Users 
                            select new { SubstringOfMyArrayWithLength = user.MyArray.Substring(1, 2) }"
                    },
                    Type = IndexType.Map,
                    Name = "TestIndexSubstringOfArray2"
                }));
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = {
                        @"from user in docs.Users 
                            select new { SubstringOfMyList = user.MyList.Substring(0) }"
                    },
                    Type = IndexType.Map,
                    Name = "TestIndexSubstringOfList1"
                }));
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = {
                        @"from user in docs.Users 
                            select new { SubstringOfMyListWithLength = user.MyList.Substring(1, 2) }"
                    },
                    Type = IndexType.Map,
                    Name = "TestIndexSubstringOfList2"
                }));
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = {
                        @"from user in docs.Users 
                            select new { SubstringOfMyHashSet = user.MyHashSet.Substring(0) }"
                    },
                    Type = IndexType.Map,
                    Name = "TestIndexSubstringOfHashSet1"
                }));
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = {
                        @"from user in docs.Users 
                            select new { SubstringOfMyHashSetWithLength = user.MyHashSet.Substring(1, 2) }"
                    },
                    Type = IndexType.Map,
                    Name = "TestIndexSubstringOfHashSet2"
                }));
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "TestIndexSubstringOfCollection1",
                    Maps =
                    {
                        @"from user in docs.Users 
                                select new { SubstringOfMyCollection = user.MyCollection.Substring(0) }"
                    },
                    Type = IndexType.Map
                }));
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "TestIndexSubstringOfCollection2",
                    Maps =
                    {
                        @"from user in docs.Users 
                                select new { SubstringOfMyCollectionWithLength = user.MyCollection.Substring(1, 2) }"
                    },
                    Type = IndexType.Map
                }));
                WaitForIndexing(store, allowErrors: true);

                Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexSubstringOfArray1" }))[0].Errors.Length);
                Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexSubstringOfList1" }))[0].Errors.Length);
                Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexSubstringOfHashSet1" }))[0].Errors.Length);
                Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexSubstringOfCollection1" }))[0].Errors.Length);
                Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexSubstringOfArray2" }))[0].Errors.Length);
                Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexSubstringOfList2" }))[0].Errors.Length);
                Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexSubstringOfHashSet2" }))[0].Errors.Length);
                Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexSubstringOfCollection2" }))[0].Errors.Length);
            }
        }

        [Fact]
        public void ShouldMapIndexWithIndexOf()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = 111;
                    session.Store(_user);
                    session.SaveChanges();

                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Maps =
                        {
                            @"from user in docs.Users 
                                select new {
                                            IndexOfMyName = user.MyName.IndexOf('o'),
                                            IndexOfMyNameWithIndex = user.MyName.IndexOf('o', 1),
                                            IndexOfMyNameWithIndexAndCount = user.MyName.IndexOf('o', 1, 3),
                                            IndexOfMyNameWithString = user.MyName.IndexOf(""go""),

                                            IndexOfMyTimeSpan = user.MyTimeSpan.IndexOf('1'),
                                            IndexOfMyTimeSpanWithIndex = user.MyTimeSpan.IndexOf('1', 1),
                                            IndexOfMyTimeSpanWithIndexAndCount = user.MyTimeSpan.IndexOf('1', 1, 3),
                                            IndexOfMyTimeSpanWithString = user.MyTimeSpan.IndexOf(""12""),
                                            IndexOfMyDateTime = user.MyDateTime.IndexOf('1'),
                                            IndexOfMyDateTimeWithIndex = user.MyDateTime.IndexOf('1', 1),
                                            IndexOfMyDateTimeWithIndexAndCount = user.MyDateTime.IndexOf('1', 1, 3),
                                            IndexOfMyDateTimeWithString = user.MyDateTime.IndexOf(""12""),
                                            IndexOfMyDateTimeOffset = user.MyDateTimeOffset.IndexOf('1'),
                                            IndexOfMyDateTimeOffsetWithIndex = user.MyDateTimeOffset.IndexOf('1', 1),
                                            IndexOfMyDateTimeOffsetWithIndexAndCount = user.MyDateTimeOffset.IndexOf('1', 1, 3),
                                            IndexOfMyDateTimeOffsetWithString = user.MyDateTimeOffset.IndexOf(""12""),

                                            IndexOfMyArray = user.MyArray.IndexOf(7),
                                            IndexOfMyArrayWithIndex = user.MyArray.IndexOf(10, 5),
                                            IndexOfMyArrayWithIndexAndCount = user.MyArray.IndexOf(4, 1, 4),
                                            IndexOfMyArrayWithString = user.MyArray.IndexOf(""5""),

                                            IndexOfMyList = user.MyList.IndexOf(""ListItem 4""),
                                            IndexOfMyListWithIndex = user.MyList.IndexOf(""ListItem 7"", 5),
                                            IndexOfMyListWithIndexAndCount = user.MyList.IndexOf(""ListItem 4"", 1, 4),
                                            IndexOfMyListWithString = user.MyList.IndexOf(""5""),

                                            IndexOfMyHashSet = user.MyHashSet.IndexOf(""HashSetItem 4""),
                                            IndexOfMyHashSetWithIndex = user.MyHashSet.IndexOf(""HashSetItem 7"", 5),
                                            IndexOfMyHashSetWithIndexAndCount = user.MyHashSet.IndexOf(""HashSetItem 4"", 1, 4),
                                            IndexOfMyHashSetWithString = user.MyHashSet.IndexOf(""5""),

                                            IndexOfMyCollection = user.MyCollection.IndexOf(""CollectionItem 4""),
                                            IndexOfMyCollectionWithIndex = user.MyCollection.IndexOf(""CollectionItem 7"", 5),
                                            IndexOfMyCollectionWithIndexAndCount = user.MyCollection.IndexOf(""CollectionItem 4"", 1, 4),
                                            IndexOfMyCollectionWithString = user.MyCollection.IndexOf(""5""),
                                            }"
                        },
                        Type = IndexType.Map,
                        Name = "TestIndexIndexOf"
                    }));

                    WaitForIndexing(store);

                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyName == _user.MyName.IndexOf('o')).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyNameWithIndex == _user.MyName.IndexOf('o', 1)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyNameWithIndexAndCount == _user.MyName.IndexOf('o', 1, 3)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyNameWithString == _user.MyName.IndexOf("go")).OfType<User>().ToArray().Length);

                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyTimeSpan == _user.MyTimeSpan.ToString().IndexOf('1')).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyTimeSpanWithIndex == _user.MyTimeSpan.ToString().IndexOf('1', 1)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyTimeSpanWithIndexAndCount == _user.MyTimeSpan.ToString().IndexOf('1', 1, 3)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyTimeSpanWithString == _user.MyTimeSpan.ToString().IndexOf("12")).OfType<User>().ToArray().Length);

                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyDateTime == _user.MyDateTime.ToString(CultureInfo.InvariantCulture).IndexOf('1')).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyDateTimeWithIndex == _user.MyDateTime.ToString(CultureInfo.InvariantCulture).IndexOf('1', 1)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyDateTimeWithIndexAndCount == _user.MyDateTime.ToString(CultureInfo.InvariantCulture).IndexOf('1', 1, 3)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyDateTimeWithString == _user.MyDateTime.ToString(CultureInfo.InvariantCulture).IndexOf("12")).OfType<User>().ToArray().Length);

                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyDateTimeOffset == _user.MyDateTimeOffset.ToString(CultureInfo.InvariantCulture).IndexOf('1')).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyDateTimeOffsetWithIndex == _user.MyDateTimeOffset.ToString(CultureInfo.InvariantCulture).IndexOf('1', 1)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyDateTimeOffsetWithIndexAndCount == _user.MyDateTimeOffset.ToString(CultureInfo.InvariantCulture).IndexOf('1', 1, 3)).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyDateTimeOffsetWithString == _user.MyDateTimeOffset.ToString(CultureInfo.InvariantCulture).IndexOf("12")).OfType<User>().ToArray().Length);

                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyArray == -1).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyArrayWithIndex == -1).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyArrayWithIndexAndCount == -1).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyArrayWithString == -1).OfType<User>().ToArray().Length);

                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyList == 3).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyListWithIndex == 6).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyListWithIndexAndCount == 3).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyListWithString == -1).OfType<User>().ToArray().Length);

                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyHashSet == 3).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyHashSetWithIndex == 6).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyHashSetWithIndexAndCount == 3).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyHashSetWithString == -1).OfType<User>().ToArray().Length);

                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyCollection == 3).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyCollectionWithIndex == 6).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyCollectionWithIndexAndCount == 3).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexIndexOf").Where(x => x.IndexOfMyCollectionWithString == -1).OfType<User>().ToArray().Length);
                }
            }
        }

        [Fact]
        public void ShouldThrowOnMapIndexWithIndexOf()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(_user);
                    session.SaveChanges();

                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Name = "TestIndexIndexOfString1",
                        Maps =
                        {
                            @"from user in docs.Users 
                                select new { IndexOfMyName = user.MyName.IndexOf(5) }"
                        },
                        Type = IndexType.Map
                    }));
                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Name = "TestIndexIndexOfString2",
                        Maps =
                        {
                            @"from user in docs.Users 
                                select new { IndexOfMyName = user.MyName.IndexOf(5, 1) }"
                        },
                        Type = IndexType.Map
                    }));
                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Name = "TestIndexIndexOfString3",
                        Maps =
                        {
                            @"from user in docs.Users 
                                select new { IndexOfMyName = user.MyName.IndexOf(5, 1, 5) }"
                        },
                        Type = IndexType.Map
                    }));
                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Name = "TestIndexIndexOfString4",
                        Maps =
                        {
                            @"from user in docs.Users 
                                select new { IndexOfMyName = user.MyName.IndexOf(user.MyUser) }"
                        },
                        Type = IndexType.Map
                    }));
                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Name = "TestIndexIndexOfString5",
                        Maps =
                        {
                            @"from user in docs.Users 
                                select new { IndexOfMyUsersList = user.MyUsersList.IndexOf('x', StringComparison.Ordinal) }"
                        },
                        Type = IndexType.Map
                    }));

                    WaitForIndexing(store, allowErrors: true);

                    Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexIndexOfString1" }))[0].Errors.Length);
                    Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexIndexOfString2" }))[0].Errors.Length);
                    Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexIndexOfString3" }))[0].Errors.Length);
                    Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexIndexOfString4" }))[0].Errors.Length);
                    Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexIndexOfString5" }))[0].Errors.Length);
                }
            }
        }

        [Fact]
        public void ShouldMapIndexWithStartsWith()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    _user.MyDateTime = DateTime.MinValue;
                    _user.MyDateTimeOffset = DateTimeOffset.MinValue;
                    session.Store(_user);
                    session.SaveChanges();

                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Maps =
                        {
                            @"from user in docs.Users 
                                select new {
                                            StartsWithOfMyNameChar = user.MyName.StartsWith('E'),
                                            StartsWithOfMyNameString = user.MyName.StartsWith(""Ego""),
											
											StartsWithOfMyTimeSpanChar = user.MyTimeSpan.StartsWith('1'),
                                            StartsWithOfMyTimeSpanString = user.MyTimeSpan.StartsWith(""10""),
											StartsWithOfMyDateTimeChar = user.MyDateTime.StartsWith('0'),
                                            StartsWithOfMyDateTimeString = user.MyDateTime.StartsWith(""01/01""),
											StartsWithOfMyDateTimeOffsetChar = user.MyDateTimeOffset.StartsWith('0'),
                                            StartsWithOfMyDateTimeOffsetString = user.MyDateTimeOffset.StartsWith(""01/01""),
											StartsWithOfMyUserChar = user.MyUser.StartsWith('{'),
                                            StartsWithOfMyUserString = user.MyUser.StartsWith(""{\""Name\"":""),
											
											StartsWithOfMyIntChar = user.MyInt.StartsWith('3'),
                                            StartsWithOfMyIntString = user.MyInt.StartsWith(""32""),
											StartsWithOfMyLongChar = user.MyLong.StartsWith('1'),
                                            StartsWithOfMyLongString = user.MyLong.StartsWith(""123""),
											StartsWithOfMyDoubleChar = user.MyDouble.StartsWith('1'),
                                            StartsWithOfMyDoubleString = user.MyDouble.StartsWith(""123.""),
											StartsWithOfMyFloatChar = user.MyFloat.StartsWith('4'),
                                            StartsWithOfMyFloatString = user.MyFloat.StartsWith(""4.1""),
											StartsWithOfMyCharChar = user.MyChar.StartsWith('a'),
                                            StartsWithOfMyCharString = user.MyChar.StartsWith(""a""),
                                            }"
                        },
                        Type = IndexType.Map,
                        Name = "TestIndexStartsWith"
                    }));

                    WaitForIndexing(store);

                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyNameChar == _user.MyName.StartsWith('E')).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyNameString == _user.MyName.StartsWith("Ego")).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyTimeSpanChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyTimeSpanString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyDateTimeChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyDateTimeString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyDateTimeOffsetChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyDateTimeOffsetString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyUserChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyUserString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyIntChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyIntString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyLongChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyLongString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyDoubleChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyDoubleString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyFloatChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyFloatString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyCharChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexStartsWith").Where(x => x.StartsWithOfMyCharString == true).OfType<User>().ToArray().Length);
                }
            }
        }

        [Fact]
        public void ShouldMapIndexWithEndsWith()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    _user.MyDateTime = DateTime.MinValue;
                    _user.MyDateTimeOffset = DateTimeOffset.MinValue;
                    session.Store(_user);
                    session.SaveChanges();

                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Maps =
                        {
                            @"from user in docs.Users 
                                select new {
                                            EndsWithOfMyNameChar = user.MyName.EndsWith('r'),
                                            EndsWithOfMyNameString = user.MyName.EndsWith(""gor""),
											
											EndsWithOfMyTimeSpanChar = user.MyTimeSpan.EndsWith('0'),
                                            EndsWithOfMyTimeSpanString = user.MyTimeSpan.EndsWith("":30""),
											EndsWithOfMyDateTimeChar = user.MyDateTime.EndsWith('0'),
                                            EndsWithOfMyDateTimeString = user.MyDateTime.EndsWith(""00:00""),
											EndsWithOfMyDateTimeOffsetChar = user.MyDateTimeOffset.EndsWith('0'),
                                            EndsWithOfMyDateTimeOffsetString = user.MyDateTimeOffset.EndsWith(""+00:00""),
											EndsWithOfMyUserChar = user.MyUser.EndsWith('}'),
                                            EndsWithOfMyUserString = user.MyUser.EndsWith(""\""Number\"":322}""),
											
											EndsWithOfMyIntChar = user.MyInt.EndsWith('2'),
                                            EndsWithOfMyIntString = user.MyInt.EndsWith(""22""),
											EndsWithOfMyLongChar = user.MyLong.EndsWith('9'),
                                            EndsWithOfMyLongString = user.MyLong.EndsWith(""789""),
											EndsWithOfMyDoubleChar = user.MyDouble.EndsWith('6'),
                                            EndsWithOfMyDoubleString = user.MyDouble.EndsWith("".456""),
											EndsWithOfMyFloatChar = user.MyFloat.EndsWith('7'),
                                            EndsWithOfMyFloatString = user.MyFloat.EndsWith(""51367""),
											EndsWithOfMyCharChar = user.MyChar.EndsWith('a'),
                                            EndsWithOfMyCharString = user.MyChar.EndsWith(""a""),
                                            }"
                        },
                        Type = IndexType.Map,
                        Name = "TestIndexEndsWith"
                    }));

                    WaitForIndexing(store);

                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyNameChar == _user.MyName.EndsWith('r')).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyNameString == _user.MyName.EndsWith("gor")).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyTimeSpanChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyTimeSpanString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyDateTimeChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyDateTimeString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyDateTimeOffsetChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyDateTimeOffsetString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyUserChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyUserString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyIntChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyIntString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyLongChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyLongString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyDoubleChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyDoubleString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyFloatChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyFloatString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyCharChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexEndsWith").Where(x => x.EndsWithOfMyCharString == true).OfType<User>().ToArray().Length);
                }
            }
        }

        [Fact]
        public void ShouldThrowOnMapIndexWithEndsOrStartWith()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(_user);
                    session.SaveChanges();

                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Name = "TestIndexStartsWithOfString",
                        Maps =
                        {
                            @"from user in docs.Users 
                                select new { StartsWithOfMyNameChar = user.MyName.StartsWith(user.MyList) }"
                        },
                        Type = IndexType.Map
                    }));
                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Name = "TestIndexEndsWithOfString",
                        Maps =
                        {
                            @"from user in docs.Users 
                                select new { EndsWithOfMyNameChar = user.MyName.EndsWith(user.MyUser) }"
                        },
                        Type = IndexType.Map
                    }));
                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Name = "TestIndexStartsWithOfArray",
                        Maps =
                        {
                            @"from user in docs.Users 
                                select new { StartsWithOfMyArray = user.MyArray.StartsWith('5') }"
                        },
                        Type = IndexType.Map
                    }));
                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Name = "TestIndexEndsWithOfArray",
                        Maps =
                        {
                            @"from user in docs.Users 
                                select new { EndsWithOfMyArray = user.MyArray.EndsWith(""322"") }"
                        },
                        Type = IndexType.Map
                    }));

                    WaitForIndexing(store, allowErrors: true);

                    Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexStartsWithOfString" }))[0].Errors.Length);
                    Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexEndsWithOfString" }))[0].Errors.Length);
                    Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexStartsWithOfArray" }))[0].Errors.Length);
                    Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexEndsWithOfArray" }))[0].Errors.Length);
                }
            }
        }


        [Fact]
        public void ShouldMapIndexWithContains()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    _user.MyDateTime = DateTime.MinValue;
                    _user.MyDateTimeOffset = DateTimeOffset.MinValue;
                    session.Store(_user);
                    session.SaveChanges();
                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Maps =
                        {
                            @"from user in docs.Users 
                                select new {
                                            ContainsOfMyNameChar = user.MyName.Contains('r'),
                                            ContainsOfMyNameString = user.MyName.Contains(""gor""),
											
											ContainsOfMyTimeSpanChar = user.MyTimeSpan.Contains('0'),
                                            ContainsOfMyTimeSpanString = user.MyTimeSpan.Contains("":30""),
											ContainsOfMyDateTimeChar = user.MyDateTime.Contains('0'),
                                            ContainsOfMyDateTimeString = user.MyDateTime.Contains(""00:00""),
											ContainsOfMyDateTimeOffsetChar = user.MyDateTimeOffset.Contains('0'),
                                            ContainsOfMyDateTimeOffsetString = user.MyDateTimeOffset.Contains(""+00:00""),
											ContainsOfMyUserChar = user.MyUser.Contains('}'),
                                            ContainsOfMyUserString = user.MyUser.Contains(""\""Number\"":322}""),
											
											ContainsOfMyIntChar = user.MyInt.Contains('2'),
                                            ContainsOfMyIntString = user.MyInt.Contains(""22""),
											ContainsOfMyLongChar = user.MyLong.Contains('9'),
                                            ContainsOfMyLongString = user.MyLong.Contains(""789""),
											ContainsOfMyDoubleChar = user.MyDouble.Contains('6'),
                                            ContainsOfMyDoubleString = user.MyDouble.Contains("".456""),
											ContainsOfMyFloatChar = user.MyFloat.Contains('7'),
                                            ContainsOfMyFloatString = user.MyFloat.Contains(""51367""),
											ContainsOfMyCharChar = user.MyChar.Contains('a'),
                                            ContainsOfMyCharString = user.MyChar.Contains(""a""),

                                            ContainsOfMyArray = user.MyArray.Contains(7),
                                            ContainsOfMyShortArray = user.MyShortArray.Contains(2),
                                            ContainsOfMyDoubleArray = user.MyDoubleArray.Contains(1.2),
                                            ContainsOfMyFloatArray = user.MyFloatArray.Contains(0.2f),
                                            ContainsOfMyList = user.MyList.Contains(""ListItem 4""),
                                            ContainsOfMyHashSet = user.MyHashSet.Contains(""HashSetItem 4""),
                                            ContainsOfMyCollection = user.MyCollection.Contains(""CollectionItem 4"")
                                            }"
                        },
                        Type = IndexType.Map,
                        Name = "TestIndexContains"
                    }));

                    WaitForIndexing(store);

                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyNameChar == _user.MyName.EndsWith('r')).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyNameString == _user.MyName.EndsWith("gor")).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyTimeSpanChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyTimeSpanString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyDateTimeChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyDateTimeString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyDateTimeOffsetChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyDateTimeOffsetString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyUserChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyUserString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyIntChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyIntString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyLongChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyLongString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyDoubleChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyDoubleString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyFloatChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyFloatString == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyCharChar == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyCharString == true).OfType<User>().ToArray().Length);
                    
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyArray == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyShortArray == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyDoubleArray == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyFloatArray == true).OfType<User>().ToArray().Length);

                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyList == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyHashSet == true).OfType<User>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexContains").Where(x => x.ContainsOfMyCollection == true).OfType<User>().ToArray().Length);
                }
            }
        }

        [Fact]
        public void ShouldThrowOnMapIndexWithContains()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(_user);
                    session.SaveChanges();

                    store.Maintenance.Send(new PutIndexesOperation(new []{ new IndexDefinition
                    {
                        Name = "TestIndexContainsOfString1",
                        Maps =
                        {
                            @"from user in docs.Users 
                                select new { ContainsOfMyNameChar = user.MyName.StartsWith(user.MyList) }"
                        },
                        Type = IndexType.Map
                    },
                        new IndexDefinition
                        {
                            Name = "TestIndexContainsOfString2",
                            Maps =
                            {
                                @"from user in docs.Users 
                                select new { ContainsOfMyNameChar2 = user.MyName.StartsWith(user.MyUser) }"
                            },
                            Type = IndexType.Map
                        }
                    }));

                    WaitForIndexing(store, allowErrors: true);
                    Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexContainsOfString1" }))[0].Errors.Length);
                    Assert.Equal(1, store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "TestIndexContainsOfString2" }))[0].Errors.Length);
                }
            }
        }

        public class User
        {
            public string MyName { get; set; }

            public TimeSpan MyTimeSpan { get; set; }
            public DateTime MyDateTime { get; set; }
            public DateTimeOffset MyDateTimeOffset { get; set; }

            public User2 MyUser { get; set; }

            public Array MyArray { get; set; }
            public Array MyShortArray { get; set; }
            public Array MyDoubleArray { get; set; }
            public Array MyFloatArray { get; set; }
            public string[] MyStringArray { get; set; }
            public char[] MyCharArray { get; set; }

            public List<string> MyList { get; set; }
            public List<int> MyIntegerList { get; set; }
            public List<User2> MyUsersList { get; set; }

            public HashSet<string> MyHashSet { get; set; }
            public Collection<string> MyCollection { get; set; }

            public short MyShort { get; set; }
            public int MyInt { get; set; }
            public long MyLong { get; set; }
            public double MyDouble { get; set; }
            public float MyFloat { get; set; }
            public char MyChar { get; set; }
        }

        public class User2
        {
            public string Name { get; set; }
            public int Number { get; set; }
        }


        public class Result
        {
            public string SubstringOfMyName { get; set; }
            public string SubstringOfMyNameWithLength { get; set; }

            public string SubstringOfMyTimeSpan { get; set; }
            public string SubstringOfMyTimeSpanWithLength { get; set; }
            public string SubstringOfMyDateTime { get; set; }
            public string SubstringOfMyDateTimeWithLength { get; set; }
            public string SubstringOfMyDateTimeOffset { get; set; }
            public string SubstringOfMyDateTimeOffsetWithLength { get; set; }

            public string SubstringOfMyUser { get; set; }
            public string SubstringOfMyUserWithLength { get; set; }
            public string SubstringOfMyInt { get; set; }
            public string SubstringOfMyIntWithLength { get; set; }
            public string SubstringOfMyLongWithLength { get; set; }
            public string SubstringOfMyLong { get; set; }
            public string SubstringOfMyFloat { get; set; }
            public string SubstringOfMyFloatWithLength { get; set; }
            public string SubstringOfMyDouble { get; set; }
            public string SubstringOfMyDoubleWithLength { get; set; }

            public int IndexOfMyName { get; set; }
            public int IndexOfMyNameWithString { get; set; }
            public int IndexOfMyNameWithIndex { get; set; }
            public int IndexOfMyNameWithIndexAndCount { get; set; }

            public int IndexOfMyTimeSpan { get; set; }
            public int IndexOfMyTimeSpanWithIndex { get; set; }
            public int IndexOfMyTimeSpanWithIndexAndCount { get; set; }
            public int IndexOfMyTimeSpanWithString { get; set; }
            public int IndexOfMyDateTime { get; set; }
            public int IndexOfMyDateTimeWithIndex { get; set; }
            public int IndexOfMyDateTimeWithIndexAndCount { get; set; }
            public int IndexOfMyDateTimeWithString { get; set; }
            public int IndexOfMyDateTimeOffset { get; set; }
            public int IndexOfMyDateTimeOffsetWithIndex { get; set; }
            public int IndexOfMyDateTimeOffsetWithIndexAndCount { get; set; }
            public int IndexOfMyDateTimeOffsetWithString { get; set; }

            public int IndexOfMyArray { get; set; }
            public int IndexOfMyArrayWithIndex { get; set; }
            public int IndexOfMyArrayWithIndexAndCount { get; set; }
            public int IndexOfMyArrayWithString { get; set; }
            public int IndexOfMyList { get; set; }
            public int IndexOfMyListWithIndex { get; set; }
            public int IndexOfMyListWithIndexAndCount { get; set; }
            public int IndexOfMyListWithString { get; set; }
            public int IndexOfMyHashSet { get; set; }
            public int IndexOfMyHashSetWithIndex { get; set; }
            public int IndexOfMyHashSetWithIndexAndCount { get; set; }
            public int IndexOfMyHashSetWithString { get; set; }
            public int IndexOfMyCollection { get; set; }
            public int IndexOfMyCollectionWithIndex { get; set; }
            public int IndexOfMyCollectionWithIndexAndCount { get; set; }
            public int IndexOfMyCollectionWithString { get; set; }
            public int IndexOfMyUsersList { get; set; }


            public bool StartsWithOfMyNameChar { get; set; }
            public bool StartsWithOfMyNameString { get; set; }
            public bool StartsWithOfMyTimeSpanChar { get; set; }
            public bool StartsWithOfMyTimeSpanString { get; set; }
            public bool StartsWithOfMyDateTimeChar { get; set; }
            public bool StartsWithOfMyDateTimeString { get; set; }
            public bool StartsWithOfMyDateTimeOffsetChar { get; set; }
            public bool StartsWithOfMyDateTimeOffsetString { get; set; }
            public bool StartsWithOfMyUserChar { get; set; }
            public bool StartsWithOfMyUserString { get; set; }
            public bool StartsWithOfMyIntChar { get; set; }
            public bool StartsWithOfMyIntString { get; set; }
            public bool StartsWithOfMyLongChar { get; set; }
            public bool StartsWithOfMyLongString { get; set; }
            public bool StartsWithOfMyDoubleChar { get; set; }
            public bool StartsWithOfMyDoubleString { get; set; }
            public bool StartsWithOfMyFloatChar { get; set; }
            public bool StartsWithOfMyFloatString { get; set; }
            public bool StartsWithOfMyCharChar { get; set; }
            public bool StartsWithOfMyCharString { get; set; }
            public bool EndsWithOfMyNameChar { get; set; }
            public bool EndsWithOfMyNameString { get; set; }
            public bool EndsWithOfMyTimeSpanChar { get; set; }
            public bool EndsWithOfMyTimeSpanString { get; set; }
            public bool EndsWithOfMyDateTimeChar { get; set; }
            public bool EndsWithOfMyDateTimeString { get; set; }
            public bool EndsWithOfMyDateTimeOffsetChar { get; set; }
            public bool EndsWithOfMyDateTimeOffsetString { get; set; }
            public bool EndsWithOfMyUserChar { get; set; }
            public bool EndsWithOfMyUserString { get; set; }
            public bool EndsWithOfMyIntChar { get; set; }
            public bool EndsWithOfMyIntString { get; set; }
            public bool EndsWithOfMyLongChar { get; set; }
            public bool EndsWithOfMyLongString { get; set; }
            public bool EndsWithOfMyDoubleChar { get; set; }
            public bool EndsWithOfMyDoubleString { get; set; }
            public bool EndsWithOfMyFloatChar { get; set; }
            public bool EndsWithOfMyFloatString { get; set; }
            public bool EndsWithOfMyCharChar { get; set; }
            public bool EndsWithOfMyCharString { get; set; }
            public bool EndsWithOfMyArray { get; set; }

            public bool ContainsOfMyNameChar { get; set; }
            public bool ContainsOfMyNameString { get; set; }
            public bool ContainsOfMyTimeSpanChar { get; set; }
            public bool ContainsOfMyTimeSpanString { get; set; }
            public bool ContainsOfMyDateTimeChar { get; set; }
            public bool ContainsOfMyDateTimeString { get; set; }
            public bool ContainsOfMyDateTimeOffsetChar { get; set; }
            public bool ContainsOfMyDateTimeOffsetString { get; set; }
            public bool ContainsOfMyUserChar { get; set; }
            public bool ContainsOfMyUserString { get; set; }
            public bool ContainsOfMyIntChar { get; set; }
            public bool ContainsOfMyIntString { get; set; }
            public bool ContainsOfMyLongChar { get; set; }
            public bool ContainsOfMyLongString { get; set; }
            public bool ContainsOfMyDoubleChar { get; set; }
            public bool ContainsOfMyDoubleString { get; set; }
            public bool ContainsOfMyFloatChar { get; set; }
            public bool ContainsOfMyFloatString { get; set; }
            public bool ContainsOfMyCharChar { get; set; }
            public bool ContainsOfMyCharString { get; set; }

            public bool ContainsOfMyArray { get; set; }
            public bool ContainsOfMyDoubleArray { get; set; }
            public bool ContainsOfMyShortArray { get; set; }
            public bool ContainsOfMyFloatArray { get; set; }

            public bool ContainsOfMyList { get; set; }
            public bool ContainsOfMyHashSet { get; set; }
            public bool ContainsOfMyCollection { get; set; }
        }

        private static readonly DateTime _dateTime = DateTime.Now;
        private static readonly TimeSpan _timeSpan = TimeSpan.Parse("10:20:30");
        private static readonly DateTimeOffset _dateTimeOffset = DateTimeOffset.Now;

        private User _user = new User
        {
            MyName = "Egor",

            MyTimeSpan = _timeSpan,
            MyDateTime = _dateTime,
            MyDateTimeOffset = _dateTimeOffset,

            MyUser = new User2 { Name = "Egor2", Number = 322 },

            MyArray = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 },
            MyShortArray = new short[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 },
            MyDoubleArray = new double[] { 0.2, 1.2, 2.3, 3.4, 4.5, 5.6, 6.7, 7.8, 8.9, 9.10, 10.11 },
            MyFloatArray = new float[] { 0.2f, 1.2f, 2.3f, 3.4f, 4.5f, 5.6f, 6.7f, 7.8f, 8.9f, 9.10f, 10.11f },
            MyStringArray = new[] { "abc", "def", "ghi", "jkl", "mno", "pqr", "stu", "vw", "xyz" },
            MyCharArray = new[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j' },

            MyList = new List<string> { "ListItem 1", "ListItem 2", "ListItem 3", "ListItem 4", "ListItem 5", "ListItem 6", "ListItem 7" },
            MyIntegerList = new List<int> { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 },
            MyUsersList = new List<User2> { new User2 { Name = "user1", Number = 322 }, new User2 { Name = "user2", Number = 322 } },

            MyHashSet = new HashSet<string> { "HashSetItem 1", "HashSetItem 2", "HashSetItem 3", "HashSetItem 4", "HashSetItem 5", "HashSetItem 6", "HashSetItem 7" },
            MyCollection = new Collection<string> { "CollectionItem 1", "CollectionItem 2", "CollectionItem 3", "CollectionItem 4", "CollectionItem 5", "CollectionItem 6", "CollectionItem 7" },

            MyInt = 322,
            MyLong = 123456789,
            MyDouble = 123.456,
            MyFloat = 4.20f,
            MyChar = 'a'
        };
    }
}
