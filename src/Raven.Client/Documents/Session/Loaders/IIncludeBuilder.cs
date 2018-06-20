using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Extensions;
using Raven.Client.Documents.Queries.Facets;

namespace Raven.Client.Documents.Session.Loaders
{
    public interface IIncludeOperations<T>
    {
        IIncludeOperations<T> Counter(string name);

        IIncludeOperations<T> Counters(string[] names);

        IIncludeOperations<T> AllCounters();

        IIncludeOperations<T> Documents(string path);

        IIncludeOperations<T> Documents(Expression<Func<T, string>> path);

        IIncludeOperations<T> Documents(Expression<Func<T, IEnumerable<string>>> path);

    }

    public interface IIncludeBuilder<T>
    {
        IIncludeOperations<T> Counter(string name);

        IIncludeOperations<T> Counters(string[] names);

        IIncludeOperations<T> AllCounters();

        IIncludeOperations<T> Documents(string path);

        IIncludeOperations<T> Documents(Expression<Func<T, string>> path);

        IIncludeOperations<T> Documents(Expression<Func<T, IEnumerable<string>>> path);

    }

    public class IncludeBuilder<T> : IIncludeBuilder<T>, IIncludeOperations<T>
    {
        public List<string> DocumentsToInclude;
        public List<string> CountersToInclude;
        public bool IncludeAllCounters;

        public IIncludeOperations<T> Documents(string path)
        {
            if (DocumentsToInclude == null)
                DocumentsToInclude = new List<string>();
            DocumentsToInclude.Add(path);
            return this;
        }

        public IIncludeOperations<T> Documents(Expression<Func<T, string>> path)
        {
            return Documents(path.ToPropertyPath());
        }

        public IIncludeOperations<T> Documents(Expression<Func<T, IEnumerable<string>>> path)
        {
            return Documents(path.ToPropertyPath());
        }

        public IIncludeOperations<T> Counter(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            if (CountersToInclude == null)
                CountersToInclude = new List<string>();

            CountersToInclude.Add(name);
            return this;
        }

        public IIncludeOperations<T> Counters(string[] names)
        {
            if (CountersToInclude == null)
                CountersToInclude = new List<string>();

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

        public IIncludeOperations<T> AllCounters()
        {
            IncludeAllCounters = true;
            return this;
        }
    }
}
