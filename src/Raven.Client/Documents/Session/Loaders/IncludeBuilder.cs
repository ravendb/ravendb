using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Session.Loaders
{
    public interface IDocumentIncludeBuilder<T, out TBuilder>
    {
        TBuilder IncludeDocuments(string path);

        TBuilder IncludeDocuments(Expression<Func<T, string>> path);

        TBuilder IncludeDocuments(Expression<Func<T, IEnumerable<string>>> path);

        TBuilder IncludeDocuments<TInclude>(Expression<Func<T, string>> path);

        TBuilder IncludeDocuments<TInclude>(Expression<Func<T, IEnumerable<string>>> path);
    }

    public interface ICounterIncludeBuilder<T, out TBuilder>
    {
        TBuilder IncludeCounter(string name);

        TBuilder IncludeCounters(string[] names);

        TBuilder IncludeAllCounters();
    }

    public interface IIncludeBuilder<T, out TBuilder> : IDocumentIncludeBuilder<T, TBuilder>, ICounterIncludeBuilder<T, TBuilder>
    {
    }

    public interface IIncludeBuilder<T> : IIncludeBuilder<T, IIncludeBuilder<T>>
    {
    }

    public interface ISubscriptionIncludeBuilder<T> : IDocumentIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>
    {
    }

    public interface IQueryIncludeBuilder<T> : IIncludeBuilder<T, IQueryIncludeBuilder<T>>
    {
        IQueryIncludeBuilder<T> IncludeCounter(Expression<Func<T, string>> path, string name);

        IQueryIncludeBuilder<T> IncludeCounters(Expression<Func<T, string>> path, string[] names);

        IQueryIncludeBuilder<T> IncludeAllCounters(Expression<Func<T, string>> path);
    }

    public class IncludeBuilder
    {
        internal HashSet<string> DocumentsToInclude;

        internal string Alias;

        internal HashSet<string> CountersToInclude
        {
            get
            {
                if (CountersToIncludeBySourcePath == null)
                    return null;

                CountersToIncludeBySourcePath.TryGetValue(string.Empty, out var value);
                return value.CountersToInclude;
            }
        }

        internal bool AllCounters
        {
            get
            {
                if (CountersToIncludeBySourcePath == null)
                    return false;

                CountersToIncludeBySourcePath.TryGetValue(string.Empty, out var value);
                return value.AllCounters;
            }
        }

        internal Dictionary<string, (bool AllCounters, HashSet<string> CountersToInclude)> CountersToIncludeBySourcePath;
    }

    internal class IncludeBuilder<T> : IncludeBuilder, IQueryIncludeBuilder<T>, IIncludeBuilder<T>, ISubscriptionIncludeBuilder<T>
    {
        private readonly DocumentConventions _conventions;

        internal IncludeBuilder(DocumentConventions conventions)
        {
            _conventions = conventions;
        }

        IQueryIncludeBuilder<T> IQueryIncludeBuilder<T>.IncludeCounter(Expression<Func<T, string>> path, string name)
        {
            IncludeCounterWithAlias(path, name);
            return this;
        }

        IQueryIncludeBuilder<T> IQueryIncludeBuilder<T>.IncludeCounters(Expression<Func<T, string>> path, string[] names)
        {
            IncludeCountersWithAlias(path, names);
            return this;
        }

        IQueryIncludeBuilder<T> IQueryIncludeBuilder<T>.IncludeAllCounters(Expression<Func<T, string>> path)
        {
            IncludeAllCountersWithAlias(path);
            return this;
        }

        IQueryIncludeBuilder<T> ICounterIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeCounter(string name)
        {
            IncludeCounter(string.Empty, name);
            return this;
        }

        IQueryIncludeBuilder<T> ICounterIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeCounters(string[] names)
        {
            IncludeCounters(string.Empty, names);
            return this;
        }

        IQueryIncludeBuilder<T> ICounterIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeAllCounters()
        {
            IncludeAllCounters(string.Empty);
            return this;
        }

        ISubscriptionIncludeBuilder<T> IDocumentIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeDocuments(Expression<Func<T, string>> path)
        {
            IncludeDocuments(path.ToPropertyPath());
            return this;
        }

        ISubscriptionIncludeBuilder<T> IDocumentIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeDocuments(Expression<Func<T, IEnumerable<string>>> path)
        {
            IncludeDocuments(path.ToPropertyPath());
            return this;
        }

        ISubscriptionIncludeBuilder<T> IDocumentIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeDocuments<TInclude>(Expression<Func<T, string>> path)
        {
            IncludeDocuments(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(), _conventions));
            return this;
        }

        ISubscriptionIncludeBuilder<T> IDocumentIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeDocuments<TInclude>(Expression<Func<T, IEnumerable<string>>> path)
        {
            IncludeDocuments(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(), _conventions));
            return this;
        }

        ISubscriptionIncludeBuilder<T> IDocumentIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeDocuments(string path)
        {
            IncludeDocuments(path);
            return this;
        }

        IQueryIncludeBuilder<T> IDocumentIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeDocuments(Expression<Func<T, string>> path)
        {
            IncludeDocuments(path.ToPropertyPath());
            return this;
        }

        IQueryIncludeBuilder<T> IDocumentIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeDocuments(Expression<Func<T, IEnumerable<string>>> path)
        {
            IncludeDocuments(path.ToPropertyPath());
            return this;
        }

        IQueryIncludeBuilder<T> IDocumentIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeDocuments<TInclude>(Expression<Func<T, string>> path)
        {
            IncludeDocuments(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(), _conventions));
            return this;
        }

        IQueryIncludeBuilder<T> IDocumentIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeDocuments<TInclude>(Expression<Func<T, IEnumerable<string>>> path)
        {
            IncludeDocuments(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(), _conventions));
            return this;
        }

        IQueryIncludeBuilder<T> IDocumentIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeDocuments(string path)
        {
            IncludeDocuments(path);
            return this;
        }

        IIncludeBuilder<T> ICounterIncludeBuilder<T, IIncludeBuilder<T>>.IncludeCounter(string name)
        {
            IncludeCounter(string.Empty, name);
            return this;
        }

        IIncludeBuilder<T> ICounterIncludeBuilder<T, IIncludeBuilder<T>>.IncludeCounters(string[] names)
        {
            IncludeCounters(string.Empty, names);
            return this;
        }

        IIncludeBuilder<T> ICounterIncludeBuilder<T, IIncludeBuilder<T>>.IncludeAllCounters()
        {
            IncludeAllCounters(string.Empty);
            return this;
        }

        IIncludeBuilder<T> IDocumentIncludeBuilder<T, IIncludeBuilder<T>>.IncludeDocuments(string path)
        {
            IncludeDocuments(path);
            return this;
        }

        IIncludeBuilder<T> IDocumentIncludeBuilder<T, IIncludeBuilder<T>>.IncludeDocuments(Expression<Func<T, string>> path)
        {
            IncludeDocuments(path.ToPropertyPath());
            return this;
        }

        IIncludeBuilder<T> IDocumentIncludeBuilder<T, IIncludeBuilder<T>>.IncludeDocuments(Expression<Func<T, IEnumerable<string>>> path)
        {
            IncludeDocuments(path.ToPropertyPath());
            return this;
        }

        IIncludeBuilder<T> IDocumentIncludeBuilder<T, IIncludeBuilder<T>>.IncludeDocuments<TInclude>(Expression<Func<T, string>> path)
        {
            IncludeDocuments(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(), _conventions));
            return this;
        }

        IIncludeBuilder<T> IDocumentIncludeBuilder<T, IIncludeBuilder<T>>.IncludeDocuments<TInclude>(Expression<Func<T, IEnumerable<string>>> path)
        {
            IncludeDocuments(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(), _conventions));
            return this;
        }

        private void IncludeDocuments(string path)
        {
            if (DocumentsToInclude == null)
                DocumentsToInclude = new HashSet<string>();

            DocumentsToInclude.Add(path);
        }

        private void IncludeCounterWithAlias(Expression<Func<T, string>> path, string name)
        {
            WithAlias(path);
            IncludeCounter(path.ToPropertyPath(), name);
        }

        private void IncludeCountersWithAlias(Expression<Func<T, string>> path, string[] names)
        {
            WithAlias(path);
            IncludeCounters(path.ToPropertyPath(), names);
        }

        private void IncludeCounter(string path, string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            AssertNotAllAndAddNewEntryIfNeeded(path);

            CountersToIncludeBySourcePath[path]
                .CountersToInclude.Add(name);
        }

        private void IncludeCounters(string path, string[] names)
        {
            if (names == null)
                throw new ArgumentNullException(nameof(names));

            AssertNotAllAndAddNewEntryIfNeeded(path);

            foreach (var name in names)
            {
                if (string.IsNullOrWhiteSpace(name))
                    throw new InvalidOperationException("Counters(string[] names) : 'names' should not " +
                                                        "contain null or whitespace elements");
                CountersToIncludeBySourcePath[path]
                    .CountersToInclude.Add(name);
            }

        }

        private void IncludeAllCountersWithAlias(Expression<Func<T, string>> path)
        {
            WithAlias(path);
            IncludeAllCounters(path.ToPropertyPath());
        }

        private void IncludeAllCounters(string sourcePath)
        {
            if (CountersToIncludeBySourcePath == null)
            {
                CountersToIncludeBySourcePath = new Dictionary<string,
                    (bool, HashSet<string>)>(StringComparer.OrdinalIgnoreCase);
            }

            if (CountersToIncludeBySourcePath.TryGetValue(sourcePath, out var val) &&
                val.CountersToInclude != null)

                throw new InvalidOperationException("IIncludeBuilder : You cannot use AllCounters() after using " +
                                                    "Counter(string name) or Counters(string[] names)");

            CountersToIncludeBySourcePath[sourcePath] = (true, null);
        }

        private void AssertNotAllAndAddNewEntryIfNeeded(string path)
        {
            if (CountersToIncludeBySourcePath != null &&
                CountersToIncludeBySourcePath.TryGetValue(path, out var val) &&
                val.AllCounters)
                throw new InvalidOperationException("IIncludeBuilder : You cannot use Counter(name) after using AllCounters() ");

            if (CountersToIncludeBySourcePath == null)
            {
                CountersToIncludeBySourcePath = new Dictionary<string,
                    (bool, HashSet<string>)>(StringComparer.OrdinalIgnoreCase);
            }

            if (CountersToIncludeBySourcePath.ContainsKey(path) == false)
            {
                CountersToIncludeBySourcePath[path] = (false, new HashSet<string>(StringComparer.OrdinalIgnoreCase));
            }
        }

        private void WithAlias(Expression<Func<T, string>> path)
        {
            if (Alias == null)
                Alias = path.Parameters[0].Name;
        }
    }
}
