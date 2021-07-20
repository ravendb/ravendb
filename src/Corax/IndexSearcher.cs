using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Sparrow.Server.Compression;
using Voron;
using Voron.Impl;
using Voron.Data.Sets;
using Voron.Data.Containers;
using Corax.Queries;
using System.Collections.Generic;

namespace Corax
{
    public sealed unsafe class IndexSearcher : IDisposable
    {
        private readonly StorageEnvironment _environment;
        private readonly Transaction _transaction;

        // The reason why we want to have the transaction open for us is so that we avoid having
        // to explicitly provide the index searcher with opening semantics and also every new
        // searcher becomes essentially a unit of work which makes reusing assets tracking more explicit.
        public IndexSearcher(StorageEnvironment environment)
        {
            _environment = environment;
            _transaction = environment.ReadTransaction();
        }

        public IndexEntryReader GetReaderFor(long id)
        {
            var data = Container.Get(_transaction.LowLevelTransaction, id).ToSpan();
            int size = ZigZagEncoding.Decode<int>(data, out var len);
            return new IndexEntryReader(data.Slice(size + len));
        }

        public string GetIdentityFor(long id)
        {
            var data = Container.Get(_transaction.LowLevelTransaction, id).ToSpan();
            int size = ZigZagEncoding.Decode<int>(data, out var len);
            return Encoding.UTF8.GetString(data.Slice(len, size));
        }

        public IQueryMatch Search(string q)
        {
            var parser = new QueryParser();
            parser.Init(q);
            var query = parser.Parse();
            return Search(query.Where);
        }

        public IQueryMatch Search(QueryExpression where)
        {
            return Evaluate(@where);
        }

        private IQueryMatch Evaluate(QueryExpression where)
        {
            switch (@where)
            {
                case TrueExpression _:
                case null:
                    return null; // all docs here
                case InExpression ie:
                    return (ie.Source, ie.Values) switch
                    {
                        (FieldExpression f, List<QueryExpression> list) => EvaluateInExpression(f, list),
                        _ => throw new NotSupportedException()
                    };
                case BinaryExpression be:
                    return (be.Operator, be.Left, be.Right) switch
                    {
                        (OperatorType.Equal, FieldExpression f, ValueExpression v) => TermQuery(f.FieldValue, v.Token.Value),
                        (OperatorType.And, QueryExpression q1, QueryExpression q2) => And(Evaluate(q1), Evaluate(q2)),
                        (OperatorType.Or, QueryExpression q1, QueryExpression q2) => Or(Evaluate(q1), Evaluate(q2)),
                        _ => throw new NotSupportedException()
                    };
                default:
                    return null;
            }
        }

        private IQueryMatch EvaluateInExpression(FieldExpression f, List<QueryExpression> list)
        {
            var values = new List<string>();
            foreach (ValueExpression v in list)
                values.Add(v.Token.Value); 

            return InQuery(f.FieldValue, values);
        }

        // foreach term in 2010 .. 2020
        //     yield return TermMatch(field, term)// <-- one term , not sorted

        // userid = UID and date between 2010 and 2020 <-- 100 million terms here 
        // foo = bar and published = true

        // foo = bar
        public TermMatch TermQuery(string field, string term)
        {
            var fields = _transaction.ReadTree(IndexWriter.FieldsSlice);
            var terms = fields.CompactTreeFor(field);
            if (terms == null || terms.TryGetValue(term, out var value) == false)
                return TermMatch.CreateEmpty();

            TermMatch matches;
            if ((value & (long)TermIdMask.Set) != 0)
            {
                var setId = value & ~0b11;
                var setStateSpan = Container.Get(_transaction.LowLevelTransaction, setId).ToSpan();
                ref readonly var setState = ref MemoryMarshal.AsRef<SetState>(setStateSpan);
                var set = new Set(_transaction.LowLevelTransaction, Slices.Empty, setState);
                matches = TermMatch.YieldSet(set);
            }
            else if ((value & (long)TermIdMask.Small) != 0)
            {
                var smallSetId = value & ~0b11;
                var small = Container.Get(_transaction.LowLevelTransaction, smallSetId);
                matches = TermMatch.YieldSmall(small);
            }
            else
            {
                matches = TermMatch.YieldOnce(value);
            }

            return matches;
        }

        public MultiTermMatch InQuery(string field, List<string> inTerms)
        {
            // TODO: The IEnumerable<string> will die eventually, this is for prototyping only. 
            var fields = _transaction.ReadTree(IndexWriter.FieldsSlice);
            var terms = fields.CompactTreeFor(field);
            if (terms == null)
                return MultiTermMatch.CreateEmpty();

            return MultiTermMatch.Create(new MultiTermMatch<InTermProvider>(new InTermProvider(this, field, 0, inTerms)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BinaryMatch And<TInner, TOuter>(in TInner set1, in TOuter set2)
            where TInner : IQueryMatch
            where TOuter : IQueryMatch
        {
            // TODO: We need to create this code using a template or using typed delegates (which either way would need templating for boilerplate code generation)

            // If any of the generic types is not known to be a struct (calling from interface) the code executed will
            // do all the work to figure out what to emit. The cost is in instantiation not on execution.             
            if (set1.GetType() == typeof(TermMatch) && set2.GetType() == typeof(TermMatch))
            {
                return BinaryMatch.Create(BinaryMatch<TermMatch, TermMatch>.YieldAnd((TermMatch)(object)set1, (TermMatch)(object)set2));
            }
            else if (set1.GetType() == typeof(BinaryMatch) && set2.GetType() == typeof(TermMatch))
            {
                return BinaryMatch.Create(BinaryMatch<BinaryMatch, TermMatch>.YieldAnd((BinaryMatch)(object)set1, (TermMatch)(object)set2));
            }
            else if (set1.GetType() == typeof(TermMatch) && set2.GetType() == typeof(BinaryMatch))
            {
                return BinaryMatch.Create(BinaryMatch<TermMatch, BinaryMatch>.YieldAnd((TermMatch)(object)set1, (BinaryMatch)(object)set2));
            }
            else if (set1.GetType() == typeof(BinaryMatch) && set2.GetType() == typeof(BinaryMatch))
            {
                return BinaryMatch.Create(BinaryMatch<BinaryMatch, BinaryMatch>.YieldAnd((BinaryMatch)(object)set1, (BinaryMatch)(object)set2));
            }

            return BinaryMatch.Create(BinaryMatch<TInner, TOuter>.YieldAnd(in set1, in set2));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BinaryMatch Or<TInner, TOuter>(in TInner set1, in TOuter set2)
            where TInner : IQueryMatch
            where TOuter : IQueryMatch
        {
            // If any of the generic types is not known to be a struct (calling from interface) the code executed will
            // do all the work to figure out what to emit. The cost is in instantiation not on execution. 
            if (set1.GetType() == typeof(TermMatch) && set2.GetType() == typeof(TermMatch))
            {
                return BinaryMatch.Create(BinaryMatch<TermMatch, TermMatch>.YieldOr((TermMatch)(object)set1, (TermMatch)(object)set2));
            }
            else if (set1.GetType() == typeof(BinaryMatch) && set2.GetType() == typeof(TermMatch))
            {
                return BinaryMatch.Create(BinaryMatch<BinaryMatch, TermMatch>.YieldOr((BinaryMatch)(object)set1, (TermMatch)(object)set2));
            }
            else if (set1.GetType() == typeof(TermMatch) && set2.GetType() == typeof(BinaryMatch))
            {
                return BinaryMatch.Create(BinaryMatch<TermMatch, BinaryMatch>.YieldOr((TermMatch)(object)set1, (BinaryMatch)(object)set2));
            }
            else if (set1.GetType() == typeof(BinaryMatch) && set2.GetType() == typeof(BinaryMatch))
            {
                return BinaryMatch.Create(BinaryMatch<BinaryMatch, BinaryMatch>.YieldOr((BinaryMatch)(object)set1, (BinaryMatch)(object)set2));
            }

            return BinaryMatch.Create(BinaryMatch<TInner, TOuter>.YieldOr(in set1, in set2));
        }

        public void Dispose()
        {
            _transaction?.Dispose();
        }
    }
}
