using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Session.Loaders
{
    public interface IIncludeBuilder<T>
    {
        IIncludeBuilder<T> IncludeCounter(string name);

        IIncludeBuilder<T> IncludeCounters(string[] names);

        IIncludeBuilder<T> IncludeAllCounters();

        IIncludeBuilder<T> IncludeDocuments(string path);

        IIncludeBuilder<T> IncludeDocuments(Expression<Func<T, string>> path);

        IIncludeBuilder<T> IncludeDocuments(Expression<Func<T, IEnumerable<string>>> path);

        IIncludeBuilder<T> IncludeDocuments<TInclude>(Expression<Func<T, string>> path);

        IIncludeBuilder<T> IncludeDocuments<TInclude>(Expression<Func<T, IEnumerable<string>>> path);

    }

    internal class IncludeBuilder<T> : IIncludeBuilder<T>
    {
        public HashSet<string> DocumentsToInclude;
        public HashSet<string> CountersToInclude;
        public bool AllCounters;

        private readonly DocumentConventions _conventions;

        internal IncludeBuilder(DocumentConventions conventions)
        {
            _conventions = conventions;
        }

        public IIncludeBuilder<T> IncludeDocuments(string path)
        {
            if (DocumentsToInclude == null)
                DocumentsToInclude = new HashSet<string>();
            DocumentsToInclude.Add(path);
            return this;
        }

        public IIncludeBuilder<T> IncludeDocuments(Expression<Func<T, string>> path)
        {
            return IncludeDocuments(path.ToPropertyPath());
        }

        public IIncludeBuilder<T> IncludeDocuments(Expression<Func<T, IEnumerable<string>>> path)
        {
            return IncludeDocuments(path.ToPropertyPath());
        }


        public IIncludeBuilder<T> IncludeDocuments<TInclude>(Expression<Func<T, string>> path)
        {
            return IncludeDocuments(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(), _conventions));
        }

        public IIncludeBuilder<T> IncludeDocuments<TInclude>(Expression<Func<T, IEnumerable<string>>> path)
        {
            return IncludeDocuments(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(), _conventions));
        }

        public IIncludeBuilder<T> IncludeCounter(string name)
        {
            if (AllCounters)
                throw new InvalidOperationException("IIncludeBuilder : You cannot use Counter(string name) after using AllCounters() ");

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            if (CountersToInclude == null)
                CountersToInclude = new HashSet<string>();

            CountersToInclude.Add(name);
            return this;
        }

        public IIncludeBuilder<T> IncludeCounters(string[] names)
        {
            if (CountersToInclude == null)
                CountersToInclude = new HashSet<string>();

            if (names == null)
                throw new ArgumentNullException(nameof(names));

            foreach (var name in names)
            {
                if (string.IsNullOrWhiteSpace(name))
                    throw new InvalidOperationException("Counters(string[] names) : 'names' should not contain null or whitespace elements");
                CountersToInclude.Add(name);
            }
            return this;
        }

        public IIncludeBuilder<T> IncludeAllCounters()
        {
            if (CountersToInclude != null)
                throw new InvalidOperationException("IIncludeBuilder : You cannot use AllCounters() after using " +
                                                    "Counter(string name) or Counters(string[] names)");

            AllCounters = true;
            return this;
        }
    }
}
