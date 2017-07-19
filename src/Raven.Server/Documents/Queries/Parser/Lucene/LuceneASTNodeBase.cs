using System;
using System.Collections.Generic;
using Lucene.Net.Search;

namespace Raven.Server.Documents.Queries.Parser.Lucene
{
    public abstract class LuceneASTNodeBase
    {
        public abstract IEnumerable<LuceneASTNodeBase> Children { get; }

        public abstract global::Lucene.Net.Search.Query ToQuery(LuceneASTQueryConfiguration configuration);

        protected static string Asterisk = "*";

        public virtual void AddQueryToBooleanQuery(BooleanQuery b, LuceneASTQueryConfiguration configuration, Occur o)
        {
            var query = ToQuery(configuration);
            if (query != null)
                b.Add(query, o);
        }

        public virtual global::Lucene.Net.Search.Query ToGroupFieldQuery(LuceneASTQueryConfiguration configuration)
        {
            return ToQuery(configuration);
        }

        public PrefixOperator Prefix { get; set; }

        protected string GetPrefixString()
        {
            switch (Prefix)
            {
                case PrefixOperator.None:
                    return string.Empty;
                case PrefixOperator.Plus:
                    return "+";
                case PrefixOperator.Minus:
                    return "-";
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public enum PrefixOperator
        {
            None,
            Plus,
            Minus
        }
    }
}