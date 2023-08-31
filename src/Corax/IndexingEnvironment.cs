using System;
using System.Diagnostics.CodeAnalysis;
using Voron;

namespace Corax
{
    public sealed class IndexingEnvironment : IDisposable
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

        public void Initialize()
        {
            // We need to initialize and create all the support structures for the index in question.
            // Thinking here it may be useful to have the 'IndexingStorageOptions' to customize the type
            // of index we are creating if it does not exist. 
            
            // If it fails to create or open, will throw an exception. 
        }
    }
}
