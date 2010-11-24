namespace Raven.Management.Client.Silverlight.Client
{
    using System;
    using System.Collections.Generic;
    using Common;
    using Database;
    using Database.Data;
    using Database.Indexing;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// An async database command operations
    /// </summary>
    public interface IAsyncDatabaseCommands : IDisposable
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="callback"></param>
        void DocumentGet(string key, CallbackFunction.Load<JsonDocument> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="callback"></param>
        void DocumentGetMany(string[] keys, CallbackFunction.Load<IList<JsonDocument>> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="document"></param>
        /// <param name="callback"></param>
        void DocumentPut(JsonDocument document, CallbackFunction.SaveMany<JsonDocument> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="document"></param>
        /// <param name="callback"></param>
        void DocumentPost(JsonDocument document, CallbackFunction.SaveMany<JsonDocument> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="document"></param>
        /// <param name="callback"></param>
        void DocumentDelete(JsonDocument document, CallbackFunction.SaveMany<string> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="batchCommands"></param>
        /// <param name="batchCallback"></param>
        void DocumentBatch(IList<ICommandData> batchCommands, CallbackFunction.Batch batchCallback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="callback"></param>
        void IndexGet(string name, CallbackFunction.Load<KeyValuePair<string, IndexDefinition>> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="names"></param>
        /// <param name="existingEntities"></param>
        /// <param name="callback"></param>
        void IndexGetMany(string[] names, IDictionary<string, IndexDefinition> existingEntities,
                          CallbackFunction.Load<IDictionary<string, IndexDefinition>> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="entity"></param>
        /// <param name="callback"></param>
        void IndexPut(string name, IndexDefinition entity,
                      CallbackFunction.SaveOne<KeyValuePair<string, IndexDefinition>> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="callback"></param>
        void IndexDelete(string name, CallbackFunction.SaveOne<string> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="callback"></param>
        void AttachmentGet(string key, CallbackFunction.Load<KeyValuePair<string, Attachment>> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="callback"></param>
        void AttachmentGetAll(CallbackFunction.Load<IList<KeyValuePair<string, Attachment>>> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="callback"></param>
        void AttachmentDelete(string key, CallbackFunction.SaveMany<string> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="etag"></param>
        /// <param name="data"></param>
        /// <param name="metadata"></param>
        /// <param name="callback"></param>
        void AttachmentPut(string key, Guid? etag, byte[] data, JObject metadata,
                           CallbackFunction.SaveMany<Attachment> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="callback"></param>
        void StatisticsGet(CallbackFunction.Load<DatabaseStatistics> callback);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <param name="query"></param>
        /// <param name="includes"></param>
        /// <param name="callback"></param>
        void Query(string index, IndexQuery query, string[] includes, CallbackFunction.Load<QueryResult> callback);
    }
}