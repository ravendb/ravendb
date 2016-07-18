using System;
using Raven.Server.Documents.Indexes.Static;

namespace Raven.Server.Documents.Transformers
{
    public class CurrentTransformationScope
    {
        private readonly DocumentDatabase _documentDatabase;

        public readonly IndexingFunc TransformResults;

        [ThreadStatic]
        public static CurrentTransformationScope Current;

        public CurrentTransformationScope(IndexingFunc transformResults, DocumentDatabase documentDatabase)
        {
            TransformResults = transformResults;
            _documentDatabase = documentDatabase;
        }

        public dynamic Source;
    }
}