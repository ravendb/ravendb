using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying.Matches.TermsProviders
{
    public struct InTermsProvider(IndexSearcher searcher, in FieldMetadata field, List<string> terms) : ITermsProvider
    {
        private int _termIndex = -1;
        private readonly FieldMetadata _field = field;

        public int FillPostingListIds(Span<long> postingListIds)
        {
            int count = 0;

            while (count < postingListIds.Length && _termIndex + 1 < terms.Count)
            {
                _termIndex++;

                long containerId = searcher.GetTermPostingListId(_field, terms[_termIndex]);

                if (containerId != -1)
                    postingListIds[count++] = containerId;
            }

            return count;
        }

        public void Reset() => _termIndex = -1;

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode(nameof(InTermsProvider),
                            parameters: new Dictionary<string, string>()
                            {
                                { Constants.QueryInspectionNode.FieldName, _field.FieldName.ToString() },
                                { Constants.QueryInspectionNode.Term, string.Join(",", terms)}
                            });
        }
    }
}
