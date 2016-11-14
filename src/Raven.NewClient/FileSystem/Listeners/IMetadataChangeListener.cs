using Raven.NewClient.Abstractions.FileSystem;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.FileSystem.Listeners
{
    public interface IMetadataChangeListener
    {
        /// <summary>
        /// Invoked before the written data is sent to the server.
        /// </summary>
        /// <param name="instance">The file to affect</param>
        /// <param name="metadata">The new metadata</param>
        /// <param name="original">The original remote object metadata</param>
        /// <returns>
        /// Whatever the metadata was modified and requires us to re-send it.
        /// Returning false would mean that any request of write to the file would be 
        /// ignored in the current SaveChanges call.
        /// </returns>
        bool BeforeChange(FileHeader instance, RavenJObject metadata, RavenJObject original);

        /// <summary>
        /// Invoked after the metadata is sent to the server.
        /// </summary>
        /// <param name="instance">The updated file information.</param>
        /// <param name="metadata">The current metadata as stored in the server.</param>
        void AfterChange(FileHeader instance, RavenJObject metadata);
    }
}
