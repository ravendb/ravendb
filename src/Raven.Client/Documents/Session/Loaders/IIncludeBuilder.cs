using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Session.Loaders
{
    public interface IIncludeBuilder<T>
    {
        IIncludeBuilder<T> Counter(string name);

        IIncludeBuilder<T> Counters(string[] names);

        IIncludeBuilder<T> AllCounters();

        IIncludeBuilder<T> Documents(string path);

        IIncludeBuilder<T> Documents(Expression<Func<T, string>> path);

        IIncludeBuilder<T> Documents(Expression<Func<T, IEnumerable<string>>> path);

        IIncludeBuilder<T> Documents<TInclude>(Expression<Func<T, string>> path);

        IIncludeBuilder<T> Documents<TInclude>(Expression<Func<T, IEnumerable<string>>> path);

    }

    internal class IncludeBuilder<T> : IIncludeBuilder<T>
    {
        public HashSet<string> DocumentsToInclude;
        public HashSet<string> CountersToInclude;
        public bool IncludeAllCounters;

        private readonly DocumentConventions _conventions;

        internal IncludeBuilder(DocumentConventions conventions)
        {
            _conventions = conventions;
        }

        public IIncludeBuilder<T> Documents(string path)
        {
            if (DocumentsToInclude == null)
                DocumentsToInclude = new HashSet<string>();
            DocumentsToInclude.Add(path);
            return this;
        }

        public IIncludeBuilder<T> Documents(Expression<Func<T, string>> path)
        {
            return Documents(path.ToPropertyPath());
        }

        public IIncludeBuilder<T> Documents(Expression<Func<T, IEnumerable<string>>> path)
        {
            return Documents(path.ToPropertyPath());
        }


        public IIncludeBuilder<T> Documents<TInclude>(Expression<Func<T, string>> path)
        {
            return Documents(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(), _conventions));
        }

        public IIncludeBuilder<T> Documents<TInclude>(Expression<Func<T, IEnumerable<string>>> path)
        {
            return Documents(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(), _conventions));
        }

        public IIncludeBuilder<T> Counter(string name)
        {
            if (IncludeAllCounters)
                throw new InvalidOperationException("IIncludeBuilder : You cannot use Counter(string name) after using AllCounters() ");

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            if (CountersToInclude == null)
                CountersToInclude = new HashSet<string>();

            CountersToInclude.Add(name);
            return this;
        }

        public IIncludeBuilder<T> Counters(string[] names)
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

        public IIncludeBuilder<T> AllCounters()
        {
            if (CountersToInclude != null)
                throw new InvalidOperationException("IIncludeBuilder : You cannot use AllCounters() after using " +
                                                    "Counter(string name) or Counters(string[] names)");

            IncludeAllCounters = true;
            return this;
        }
    }
}
