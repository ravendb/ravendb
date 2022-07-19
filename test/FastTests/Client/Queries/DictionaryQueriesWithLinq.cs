using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Queries;

public class DictionaryQueriesWithLinq : RavenTestBase
{
    public DictionaryQueriesWithLinq(ITestOutputHelper output) : base(output)
    {
    }
    
    [Fact]
    public void QueryNestedDictionaryWithWhereClause2()
    {
        using (var store = GetDocumentStore())
        {
            using (var newSession = store.OpenSession())
            {
                newSession.Store(new Dictionary<string, Dictionary<string, object>> {{"Data", new Dictionary<string, object> {{"Name", "John"}}}}, "users/1");
                newSession.Store(new Dictionary<string, Dictionary<string, object>> {{"Data", new Dictionary<string, object> {{"Name", "Maciej"}}}}, "users/2");
                newSession.Store(new Dictionary<string, Dictionary<string, object>> {{"Data", new Dictionary<string, object> {{"Name", "Tarzan"}}}}, "users/3");
                newSession.SaveChanges();

                var queryResult2 = newSession.Query<Dictionary<string, Dictionary<string, object>>>()
                    .Where(x => ((string)(x["Data"]["Name"])).Equals("Tarzan"));
                Assert.Equal("from 'DictionariesOfStringsOfDictionariesOfStringsOf' where Data.Name = $p0", queryResult2.ToString());
                Assert.Equal(1, queryResult2.Count());
            }
        }
    }

    [Fact]
    public void QueryCustomDictionaryWithOrderEqualityClause()
    {
        using (var store = GetDocumentStore())
        {
            using (var newSession = store.OpenSession())
            {
                LocalizableCollection<GroupName> name = new LocalizableCollection<GroupName>
                {
                    ["en"] = new GroupName {Name = "Group", ShortName = "G"}, ["pl"] = new GroupName {Name = "Grupa", ShortName = "G"},
                };

                GroupData groupData = new GroupData {Name = name};

                newSession.Store(new Group {Data = groupData}, "group/1");
                newSession.SaveChanges();

                var queryResult = newSession.Query<Group>()
                    .OrderBy(x => x.Data.Name["pl"].Name);
                Assert.Equal("from 'Groups' order by Data.Name.pl.Name", queryResult.ToString());
                Assert.Equal(1, queryResult.ToList().Count);
            }
        }
    }

    [Fact]
    public void QueryCustomDictionaryWithWhereEqualityClause()
    {
        using (var store = GetDocumentStore())
        {
            using (var newSession = store.OpenSession())
            {
                LocalizableCollection<GroupName> name = new LocalizableCollection<GroupName>
                {
                    ["en"] = new GroupName {Name = "Group", ShortName = "G"}, ["pl"] = new GroupName {Name = "Grupa", ShortName = "G"},
                };

                GroupData groupData = new GroupData {Name = name};

                newSession.Store(new Group {Data = groupData}, "group/1");
                newSession.SaveChanges();
                WaitForUserToContinueTheTest(store);
                var query = newSession.Query<Group>()
                    .Where(x => x.Data.Name["pl"].Name == "Grupa");
                Assert.Equal(1, query.Count());
                Assert.Equal("from 'Groups' where Data.Name.pl.Name = $p0", query.ToString());
            }
        }
    }


    private class LocalizableCollection<T> : IDictionary<string, T>, IReadOnlyDictionary<string, T>
    {
        private readonly IDictionary<string, T> _data;

        public T this[string locale]
        {
            get
            {
                if (TryGetValue(locale, out T? value))
                {
                    return value;
                }

                throw new KeyNotFoundException($"Locale '{locale}' not found.");
            }
            set => _data[locale] = value;
        }

        public ICollection<string> Keys => _data.Keys;
        IEnumerable<string> IReadOnlyDictionary<string, T>.Keys => _data.Keys;

        public ICollection<T> Values => _data.Values;
        IEnumerable<T> IReadOnlyDictionary<string, T>.Values => _data.Values;

        public int Count => _data.Count;

        public bool IsReadOnly => _data.IsReadOnly;

        public LocalizableCollection()
        {
            _data = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);
        }

        public void Add(string locale, T value)
        {
            _data.Add(locale, value);
        }

        public void Add(KeyValuePair<string, T> item)
        {
            Add(item.Key, item.Value);
        }

        public void Clear()
        {
            _data.Clear();
        }

        public bool Contains(KeyValuePair<string, T> item)
        {
            return _data.Contains(item);
        }

        public bool ContainsKey(string locale)
        {
            return _data.ContainsKey(locale);
        }

        public void CopyTo(KeyValuePair<string, T>[] array, int arrayIndex)
        {
            (_data as ICollection<KeyValuePair<string, T>>).CopyTo(array, arrayIndex);
        }

        public bool TryGetValue(string locale, out T value)
        {
            return _data.TryGetValue(locale, out value);
        }

        public bool Remove(string locale)
        {
            return _data.Remove(locale);
        }

        public bool Remove(KeyValuePair<string, T> item)
        {
            return (_data as ICollection<KeyValuePair<string, T>>).Remove(item);
        }

        public IEnumerator<KeyValuePair<string, T>> GetEnumerator()
        {
            return _data.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _data.GetEnumerator();
        }
    }

    private class Group
    {
        public GroupData Data { get; set; }
    }

    private class GroupData
    {
        public LocalizableCollection<GroupName> Name { get; set; }
    }

    private class GroupName
    {
        public string Name { get; set; }
        public string ShortName { get; set; }
    }
}
