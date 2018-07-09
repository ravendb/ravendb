using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Session.Loaders
{
    public interface IIncludeBuilder<T, out TBuilder>
    {
        TBuilder IncludeCounter(string name);

        TBuilder IncludeCounters(string[] names);

        TBuilder IncludeAllCounters();

        TBuilder IncludeDocuments(string path);

        TBuilder IncludeDocuments(Expression<Func<T, string>> path);

        TBuilder IncludeDocuments(Expression<Func<T, IEnumerable<string>>> path);

        TBuilder IncludeDocuments<TInclude>(Expression<Func<T, string>> path);

        TBuilder IncludeDocuments<TInclude>(Expression<Func<T, IEnumerable<string>>> path);

    }

    public interface IIncludeBuilder<T> : IIncludeBuilder<T, IIncludeBuilder<T>>
    {
    }

    public interface IQueryIncludeBuilder<T> : IIncludeBuilder<T, IQueryIncludeBuilder<T>>
    {
        IQueryIncludeBuilder<T> IncludeCounter(Expression<Func<T, string>> path, string name);

        IQueryIncludeBuilder<T> IncludeCounters(Expression<Func<T, string>> path, string[] names);

        IQueryIncludeBuilder<T> IncludeAllCounters(Expression<Func<T, string>> path);
    }

    internal class IncludeBuilder<T> : IQueryIncludeBuilder<T>
    {
        public HashSet<string> DocumentsToInclude;
        public HashSet<string> CountersToInclude => CountersToIncludeBySourcePath[string.Empty].CountersToInclude;
        public bool AllCounters => CountersToIncludeBySourcePath[string.Empty].AllCounters;
        public string Alias;

        public Dictionary<string, (bool AllCounters, HashSet<string> CountersToInclude)> CountersToIncludeBySourcePath;


        private readonly DocumentConventions _conventions;

        internal IncludeBuilder(DocumentConventions conventions)
        {
            _conventions = conventions;
        }

        public IQueryIncludeBuilder<T> IncludeDocuments(string path)
        {
            if (DocumentsToInclude == null)
                DocumentsToInclude = new HashSet<string>();
            DocumentsToInclude.Add(path);
            return this;
        }

        public IQueryIncludeBuilder<T> IncludeDocuments(Expression<Func<T, string>> path)
        {
            return IncludeDocuments(path.ToPropertyPath());
        }

        public IQueryIncludeBuilder<T> IncludeDocuments(Expression<Func<T, IEnumerable<string>>> path)
        {
            return IncludeDocuments(path.ToPropertyPath());
        }


        public IQueryIncludeBuilder<T> IncludeDocuments<TInclude>(Expression<Func<T, string>> path)
        {
            return IncludeDocuments(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(), _conventions));
        }

        public IQueryIncludeBuilder<T> IncludeDocuments<TInclude>(Expression<Func<T, IEnumerable<string>>> path)
        {
            return IncludeDocuments(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(), _conventions));
        }

        public IQueryIncludeBuilder<T> IncludeCounter(string name)
        {
            IncludeCounter(string.Empty, name);
            return this;
        }

        public IQueryIncludeBuilder<T> IncludeCounter(Expression<Func<T, string>> path, string name)
        {
            if (Alias == null)
                Alias = path.Parameters[0].Name;

            IncludeCounter(path.ToPropertyPath(), name);
            return this;
        }

        public IQueryIncludeBuilder<T> IncludeCounters(string[] names)
        {
            IncludeCounters(string.Empty, names);
            return this;
        }

        public IQueryIncludeBuilder<T> IncludeCounters(Expression<Func<T, string>> path, string[] names)
        {
            if (Alias == null)
                Alias = path.Parameters[0].Name;

            IncludeCounters(path.ToPropertyPath(), names);
            return this;
        }

        public IQueryIncludeBuilder<T> IncludeAllCounters()
        {
            IncludeAll(string.Empty);

            return this;
        }

        public IQueryIncludeBuilder<T> IncludeAllCounters(Expression<Func<T, string>> path)
        {
            if (Alias == null)
                Alias = path.Parameters[0].Name;

            IncludeAll(path.ToPropertyPath());

            return this;
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

        private void IncludeAll(string sourcePath)
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

    }


}
