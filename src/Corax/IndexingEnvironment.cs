using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Server;
using Voron;

namespace Corax
{
    public class IndexingEnvironment : IDisposable
    {
        public readonly StorageEnvironment Storage;

        /// <summary>
        /// Returns the underlying Voron Storage options document.
        /// </summary>
        public StorageEnvironmentOptions Options => Storage.Options;

        public IndexingEnvironment([NotNull] StorageEnvironment environment)
        {
            Storage = environment;
        }

        public void Dispose()
        {
            Storage.Dispose();
        }
        //
        // public IndexWriter CreateWriter(ByteStringContext context = null, TimeSpan? timeout = null)
        // {
        //     return new (Storage, Storage.WriteTransaction(context, timeout));
        // }
        //
        // public IndexWriter CreateWriter(TransactionPersistentContext persistentContext, ByteStringContext context = null, TimeSpan? timeout = null)
        // {
        //     return new(Storage, Storage.WriteTransaction(persistentContext, context, timeout));
        // }
        //
        // public IndexSearcher CreateSearcher(TransactionPersistentContext persistentContext, ByteStringContext context = null)
        // {
        //     return new (Storage, Storage.ReadTransaction(persistentContext, context));
        // }
        // public IndexSearcher CreateSearcher(ByteStringContext context = null)
        // {
        //     return new (Storage, Storage.ReadTransaction(context));
        // }

        public void Initialize()
        {
            // We need to initialize and create all the support structures for the index in question.
            // Thinking here it may be useful to have the 'IndexingStorageOptions' to customize the type
            // of index we are creating if it does not exist. 
            
            // If it fails to create or open, will throw an exception. 
        }
    }
}
