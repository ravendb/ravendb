using System;
using System.Collections.Generic;
using Lucene.Net.Store;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Results;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public interface IBlittableObjectInstance : IObjectInstance
    {
        bool Changed { get; }
        DateTime? LastModified { get; }
        string ChangeVector { get; }
        BlittableJsonReaderObject Blittable { get; }
        string DocumentId { get; }
        Dictionary<string, BlittableJsonToken> OriginalPropertiesTypes { get; }
        Lucene.Net.Documents.Document LuceneDocument { get; set; }
        IState LuceneState { get; set; }
        Dictionary<string, IndexField> LuceneIndexFields { get; set; }
        bool LuceneAnyDynamicIndexFields { get; set; }
        ProjectionOptions Projection { get; set; }

        bool IsRoot { get; }
        SpatialResult? Distance { get; }
        float? IndexScore { get; }

        bool TryGetValue(string propertyName, out IBlittableObjectProperty value, out bool isDeleted);

        JsHandle GetOrCreate(string key);

        JsHandle GetOwnPropertyJs(string propertyName);

        JsHandle SetOwnProperty(string propertyName, JsHandle jsValue, bool toReturnCopy = true);

        bool? DeleteOwnProperty(string propertyName);

        IEnumerable<string> EnumerateOwnProperties();

        void Reset();
    }
}
