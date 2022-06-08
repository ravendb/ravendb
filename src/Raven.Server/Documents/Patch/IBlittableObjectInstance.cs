using System;
using System.Collections.Generic;
using Lucene.Net.Store;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Results;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public interface IBlittableObjectInstance<T>
        where T : struct, IJsHandle<T>
    {
        IJsEngineHandle<T> EngineHandle { get; }
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
        bool TryGetValue(string propertyName, out IBlittableObjectProperty<T> value, out bool isDeleted);

        T GetOrCreate(string key);

        T CreateJsHandle(bool keepAlive = false);

        T SetOwnProperty(string propertyName, T jsValue, bool toReturnCopy = true);

        bool? DeleteOwnProperty(string propertyName);

        IEnumerable<string> EnumerateOwnProperties();

        void Reset();
        Dictionary<string, IBlittableObjectProperty<T>> OwnValues{ get; } 
    }
}
