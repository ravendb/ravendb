//-----------------------------------------------------------------------
// <copyright file="RemoveFromIndexTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;

using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Tasks
{
    public class RemoveFromIndexTask : DocumentsTask
    {
        private HashSet<string> _keys;

        public RemoveFromIndexTask(int indexId)
            : base(indexId)
        {
            _keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        public RemoveFromIndexTask(int indexId, IEnumerable<string> keys)
            : base(indexId)
        {
            _keys = new HashSet<string>(keys, StringComparer.OrdinalIgnoreCase);
        }

        public override DocumentsTaskType Type => DocumentsTaskType.RemoveFromIndex;

        public override int NumberOfKeys
        {
            get
            {
                if (_keys == null)
                    return 0;
                return _keys.Count;
            }
        }

        public override string ToString()
        {
            return string.Format("IndexId: {0}, Keys: {1}", IndexId, string.Join(", ", _keys));
        }

        public override void Merge(DocumentsTask task)
        {
            var removeFromIndexTask = (RemoveFromIndexTask)task;
            _keys.UnionWith(removeFromIndexTask._keys);
        }

        public override void Execute()
        {
            var keysToRemove = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            //try
            //{
            //    context.TransactionalStorage.Batch(accessor =>
            //    {
            //        keysToRemove = new HashSet<string>(Keys.Where(key=>FilterDocuments(context, accessor, key)));
            //        accessor.Indexing.TouchIndexEtag(Index);
            //    });

            //    if (keysToRemove.Count == 0)
            //        return;
            //    context.IndexStorage.RemoveFromIndex(Index, keysToRemove.ToArray(), context);
            //}
            //finally
            //{
            //    context.MarkAsRemovedFromIndex(keysToRemove);
            //}
        }

        public override BlittableJsonReaderObject ToJson(MemoryOperationContext context)
        {
            var keys = new DynamicJsonArray();
            foreach (var key in _keys)
                keys.Add(key);

            var json = new DynamicJsonValue
            {
                ["IndexId"] = IndexId,
                ["Type"] = (int)Type,
                ["Keys"] = keys
            };

            return context.ReadObject(json, string.Empty, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }

        public new static RemoveFromIndexTask ToTask(BlittableJsonReaderObject json)
        {
            int indexId;
            if (json.TryGet("IndexId", out indexId) == false)
                throw new InvalidOperationException("Invalid JSON");

            BlittableJsonReaderArray arrayReader;
            if (json.TryGet("Keys", out arrayReader) == false)
                throw new InvalidOperationException("Invalid JSON");

            var keys = new string[arrayReader.Length];
            for (var i = 0; i < arrayReader.Length; i++)
                keys[i] = arrayReader.GetStringByIndex(i);

            return new RemoveFromIndexTask(indexId, keys);
        }

        ///// <summary>
        ///// We need to NOT remove documents that has been removed then added.
        ///// We DO remove documents that would be filtered out because of an Entity Name changed, though.
        ///// </summary>
        //private bool FilterDocuments(WorkContext context, IStorageActionsAccessor accessor, string key)
        //{
        //    var documentMetadataByKey = accessor.Documents.DocumentMetadataByKey(key);
        //    if (documentMetadataByKey == null)
        //        return true;
        //    var generator = context.IndexDefinitionStorage.GetViewGenerator(Index);
        //    if (generator == null)
        //        return false;

        //    if (generator.ForEntityNames.Count == 0)
        //        return false;// there is a new document and this index applies to it

        //    var entityName = documentMetadataByKey.Metadata.Value<string>(Constants.RavenEntityName);
        //    if (entityName == null)
        //        return true; // this document doesn't belong to this index any longer, need to remove it

        //    return generator.ForEntityNames.Contains(entityName) == false;
        //}

        public override DocumentsTask Clone()
        {
            return new RemoveFromIndexTask(IndexId)
            {
                _keys = new HashSet<string>(_keys, StringComparer.OrdinalIgnoreCase)
            };
        }

        public void AddKey(string key)
        {
            _keys.Add(key);
        }
    }
}
