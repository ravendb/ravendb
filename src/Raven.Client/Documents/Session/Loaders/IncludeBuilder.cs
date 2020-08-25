using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Extensions;
using Sparrow;

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

    public interface ITimeSeriesIncludeBuilder<T, out TBuilder>
    {
        TBuilder IncludeTimeSeries(string name, DateTime? from = null, DateTime? to = null);
    }

    public interface ITimeSeriesIncludeBuilder
    {
        ITimeSeriesIncludeBuilder IncludeTags();

        ITimeSeriesIncludeBuilder IncludeDocument();
    }

    public interface ISubscriptionTimeSeriesIncludeBuilder<T, out TBuilder>
    {
        TBuilder IncludeTimeSeries(string name, TimeSeriesRangeType type, TimeValue time);

        TBuilder IncludeTimeSeries(string[] names, TimeSeriesRangeType type, TimeValue time);

        TBuilder IncludeAllTimeSeries(TimeSeriesRangeType type, TimeValue time);
    }
    public interface ICompareExchangeValueIncludeBuilder<T, out TBuilder>
    {
        TBuilder IncludeCompareExchangeValue(string path);

        TBuilder IncludeCompareExchangeValue(Expression<Func<T, string>> path);

        TBuilder IncludeCompareExchangeValue(Expression<Func<T, IEnumerable<string>>> path);
    }

    public interface IIncludeBuilder<T, out TBuilder> : IDocumentIncludeBuilder<T, TBuilder>, ICounterIncludeBuilder<T, TBuilder>, ITimeSeriesIncludeBuilder<T, TBuilder>, ICompareExchangeValueIncludeBuilder<T, TBuilder>
    {
    }

    public interface IIncludeBuilder<T> : IIncludeBuilder<T, IIncludeBuilder<T>>
    {
    }

    public interface ISubscriptionIncludeBuilder<T> : IDocumentIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>, ICounterIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>, ISubscriptionTimeSeriesIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>
    {
    }

    public interface IQueryIncludeBuilder<T> : IIncludeBuilder<T, IQueryIncludeBuilder<T>>
    {
        IQueryIncludeBuilder<T> IncludeCounter(Expression<Func<T, string>> path, string name);

        IQueryIncludeBuilder<T> IncludeCounters(Expression<Func<T, string>> path, string[] names);

        IQueryIncludeBuilder<T> IncludeAllCounters(Expression<Func<T, string>> path);

        IQueryIncludeBuilder<T> IncludeTimeSeries(Expression<Func<T, string>> path, string name, DateTime from, DateTime to);
    }

    public class IncludeBuilder
    {
        internal HashSet<string> DocumentsToInclude;

        internal IEnumerable<AbstractTimeSeriesRange> TimeSeriesToInclude
        {
            get
            {
                if (TimeSeriesToIncludeBySourceAlias == null)
                    return null;

                TimeSeriesToIncludeBySourceAlias.TryGetValue(string.Empty, out var value);
                return value;
            }
        }

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

        internal Dictionary<string, HashSet<AbstractTimeSeriesRange>> TimeSeriesToIncludeBySourceAlias;
        internal HashSet<string> CompareExchangeValuesToInclude;

        internal bool IncludeTimeSeriesTags;
        internal bool IncludeTimeSeriesDocument;
    }

    internal class IncludeBuilder<T> : IncludeBuilder, IQueryIncludeBuilder<T>, IIncludeBuilder<T>, ISubscriptionIncludeBuilder<T>, ITimeSeriesIncludeBuilder
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

        ISubscriptionIncludeBuilder<T> ICounterIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeCounter(string name)
        {
            IncludeCounter(string.Empty, name);
            return this;
        }

        ISubscriptionIncludeBuilder<T> ICounterIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeCounters(string[] names)
        {
            IncludeCounters(string.Empty, names);
            return this;
        }

        ISubscriptionIncludeBuilder<T> ICounterIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeAllCounters()
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

        IIncludeBuilder<T> ITimeSeriesIncludeBuilder<T, IIncludeBuilder<T>>.IncludeTimeSeries(string name, DateTime? from, DateTime? to)
        {
            IncludeTimeSeries(string.Empty, name, from, to);
            return this;
        }

        IQueryIncludeBuilder<T> ITimeSeriesIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeTimeSeries(string name, DateTime? from, DateTime? to)
        {
            IncludeTimeSeries(string.Empty, name, from, to);
            return this;
        }

        IQueryIncludeBuilder<T> IQueryIncludeBuilder<T>.IncludeTimeSeries(Expression<Func<T, string>> path, string name, DateTime from, DateTime to)
        {
            WithAlias(path);
            IncludeTimeSeries(path.ToPropertyPath(), name, from, to);
            return this;
        }

        IQueryIncludeBuilder<T> ICompareExchangeValueIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeCompareExchangeValue(string path)
        {
            IncludeCompareExchangeValue(path);
            return this;
        }

        IQueryIncludeBuilder<T> ICompareExchangeValueIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeCompareExchangeValue(Expression<Func<T, string>> path)
        {
            IncludeCompareExchangeValue(path.ToPropertyPath());
            return this;
        }

        IQueryIncludeBuilder<T> ICompareExchangeValueIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeCompareExchangeValue(Expression<Func<T, IEnumerable<string>>> path)
        {
            IncludeCompareExchangeValue(path.ToPropertyPath());
            return this;
        }

        IIncludeBuilder<T> ICompareExchangeValueIncludeBuilder<T, IIncludeBuilder<T>>.IncludeCompareExchangeValue(string path)
        {
            IncludeCompareExchangeValue(path);
            return this;
        }

        IIncludeBuilder<T> ICompareExchangeValueIncludeBuilder<T, IIncludeBuilder<T>>.IncludeCompareExchangeValue(Expression<Func<T, string>> path)
        {
            IncludeCompareExchangeValue(path.ToPropertyPath());
            return this;
        }

        IIncludeBuilder<T> ICompareExchangeValueIncludeBuilder<T, IIncludeBuilder<T>>.IncludeCompareExchangeValue(Expression<Func<T, IEnumerable<string>>> path)
        {
            IncludeCompareExchangeValue(path.ToPropertyPath());
            return this;
        }

        ITimeSeriesIncludeBuilder ITimeSeriesIncludeBuilder.IncludeTags()
        {
            IncludeTimeSeriesTags = true;
            return this;
        }

        ITimeSeriesIncludeBuilder ITimeSeriesIncludeBuilder.IncludeDocument()
        {
            IncludeTimeSeriesDocument = true;
            return this;
        }

        public ITimeSeriesIncludeBuilder IncludeTimeSeries(string name, DateTime? from = null, DateTime? to = null)
        {
            IncludeTimeSeries(string.Empty, name, from, to);
            return this;
        }


        ISubscriptionIncludeBuilder<T> ISubscriptionTimeSeriesIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeTimeSeries(string name, TimeSeriesRangeType type, TimeValue time)
        {
            IncludeTimeSeries(string.Empty, name, type, time);
            return this;
        }

        ISubscriptionIncludeBuilder<T> ISubscriptionTimeSeriesIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeTimeSeries(string[] names, TimeSeriesRangeType type, TimeValue time)
        {
            if (names is null)
                throw new ArgumentNullException(nameof(names));

            foreach (var name in names)
                IncludeTimeSeries(string.Empty, name, type, time);

            return this;
        }

        ISubscriptionIncludeBuilder<T> ISubscriptionTimeSeriesIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeAllTimeSeries(TimeSeriesRangeType type, TimeValue time)
        {
            IncludeTimeSeries(string.Empty, string.Empty, type, time);
            return this;
        }
        private void IncludeDocuments(string path)
        {
            if (DocumentsToInclude == null)
                DocumentsToInclude = new HashSet<string>();

            DocumentsToInclude.Add(path);
        }

        private void IncludeCompareExchangeValue(string path)
        {
            if (CompareExchangeValuesToInclude == null)
                CompareExchangeValuesToInclude = new HashSet<string>();

            CompareExchangeValuesToInclude.Add(path);
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

            if (CountersToIncludeBySourcePath.TryGetValue(sourcePath, out var val) && val.CountersToInclude != null)
                throw new InvalidOperationException("IIncludeBuilder : You cannot use AllCounters() after using Counter(string name) or Counters(string[] names)");

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

        private void IncludeTimeSeries(string alias, string name, DateTime? from, DateTime? to)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            if (TimeSeriesToIncludeBySourceAlias == null)
            {
                TimeSeriesToIncludeBySourceAlias = new Dictionary<string, HashSet<AbstractTimeSeriesRange>>();
            }

            if (TimeSeriesToIncludeBySourceAlias.TryGetValue(alias, out var hashSet) == false)
            {
                TimeSeriesToIncludeBySourceAlias[alias] = hashSet = new HashSet<AbstractTimeSeriesRange>(comparer: AbstractTimeSeriesRangeComparer.Instance);
            }

            hashSet.Add(new TimeSeriesRange
            {
                Name = name,
                From = from?.EnsureUtc(),
                To = to?.EnsureUtc()
            });
        }

        private void IncludeTimeSeries(string alias, string name, TimeSeriesRangeType type, TimeValue time)
        {
            AssertValid(name, type, time);

            if (TimeSeriesToIncludeBySourceAlias == null)
            {
                TimeSeriesToIncludeBySourceAlias = new Dictionary<string, HashSet<AbstractTimeSeriesRange>>();
            }

            if (TimeSeriesToIncludeBySourceAlias.TryGetValue(alias, out var hashSet) == false)
            {
                TimeSeriesToIncludeBySourceAlias[alias] = hashSet = new HashSet<AbstractTimeSeriesRange>(comparer: AbstractTimeSeriesRangeComparer.Instance);
            }

            hashSet.Add(new TimeSeriesTimeRange
            {
                Name = name,
                Time = time,
                Type = type
            });

            static void AssertValid(string name, TimeSeriesRangeType type, TimeValue time)
            {
                if (name is null)
                    throw new ArgumentNullException(nameof(name));

                switch (type)
                {
                    case TimeSeriesRangeType.None:
                        if (time == default)
                            return;

                        throw new InvalidOperationException($"Time range type cannot be set to '{nameof(TimeSeriesRangeType.None)}' when time is specified.");
                    case TimeSeriesRangeType.Last:
                        if (time != default)
                            return;

                        throw new InvalidOperationException($"Time range type cannot be set to '{nameof(TimeSeriesRangeType.Last)}' when time is not specified.");
                    default:
                        throw new NotSupportedException($"Not supported time range type '{type}'.");
                };
            }
        }
    }

    internal class AbstractTimeSeriesRangeComparer : IEqualityComparer<AbstractTimeSeriesRange>
    {
        public static AbstractTimeSeriesRangeComparer Instance = new AbstractTimeSeriesRangeComparer();

        private AbstractTimeSeriesRangeComparer()
        {
        }

        public bool Equals(AbstractTimeSeriesRange x, AbstractTimeSeriesRange y)
        {
            return string.Equals(x?.Name, y?.Name, StringComparison.OrdinalIgnoreCase);
        }

        public int GetHashCode(AbstractTimeSeriesRange obj)
        {
            return obj.Name.GetHashCode();
        }
    }
}
