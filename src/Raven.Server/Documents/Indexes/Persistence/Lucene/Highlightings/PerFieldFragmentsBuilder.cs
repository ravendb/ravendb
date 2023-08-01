using System;
using System.Collections.Generic;
using Lucene.Net.Index;
using Lucene.Net.Search.Vectorhighlight;
using Lucene.Net.Store;
using Raven.Server.Documents.Queries;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Highlightings
{
    public sealed class PerFieldFragmentsBuilder : BaseFragmentsBuilder
    {
        private readonly Dictionary<string, (string[] PreTags, string[] PostTags)> _tagsPerField;

        private string _lastFieldName;

        public PerFieldFragmentsBuilder(IndexQueryServerSide query, JsonOperationContext context)
            : base(COLORED_PRE_TAGS, COLORED_POST_TAGS)
        {
            _tagsPerField = CreateTags(query, context);
        }

        private static Dictionary<string, (string[] PreTags, string[] PostTags)> CreateTags(IndexQueryServerSide query, JsonOperationContext context)
        {
            Dictionary<string, (string[] PreTags, string[] PostTags)> result = null;
            foreach (var highlighting in query.Metadata.Highlightings)
            {
                var options = highlighting.GetOptions(context, query.QueryParameters);
                if (options == null)
                    continue;

                var numberOfPreTags = options.PreTags?.Length ?? 0;
                var numberOfPostTags = options.PostTags?.Length ?? 0;

                if (numberOfPreTags != numberOfPostTags)
                    throw new InvalidOperationException("Number of pre-tags and post-tags must match.");

                if (numberOfPreTags == 0)
                    continue;

                if (result == null)
                    result = new Dictionary<string, (string[] PreTags, string[] PostTags)>();

                var fieldName = query.Metadata.IsDynamic 
                    ? AutoIndexField.GetSearchAutoIndexFieldName(highlighting.Field.Value) 
                    : highlighting.Field.Value;

                result[fieldName] = (options.PreTags, options.PostTags);
            }

            return result;
        }

        public override List<FieldFragList.WeightedFragInfo> GetWeightedFragInfoList(List<FieldFragList.WeightedFragInfo> src)
        {
            return src;
        }

        public override string[] CreateFragments(IndexReader reader, int docId, string fieldName, FieldFragList fieldFragList, int maxNumFragments, int fragCharSize, IState state)
        {
            _lastFieldName = fieldName;
            return base.CreateFragments(reader, docId, fieldName, fieldFragList, maxNumFragments, fragCharSize, state);
        }

        protected override string GetPreTag(int num)
        {
            if (_tagsPerField == null || _tagsPerField.TryGetValue(_lastFieldName, out var tags) == false)
                return base.GetPreTag(num);

            var n = num % tags.PreTags.Length;
            return tags.PreTags[n];
        }

        protected override string GetPostTag(int num)
        {
            if (_tagsPerField == null || _tagsPerField.TryGetValue(_lastFieldName, out var tags) == false)
                return base.GetPostTag(num);

            var n = num % tags.PostTags.Length;
            return tags.PostTags[n];
        }
    }
}
