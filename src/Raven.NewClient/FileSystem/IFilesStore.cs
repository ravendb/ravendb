using Raven.NewClient.Client.Connection;
using System.Collections.Specialized;

namespace Raven.NewClient.Client.FileSystem
{
    /// <summary>
    /// Interface for managing access to RavenFS and open sessions.
    /// </summary>
    public interface IFilesStore : IDisposalNotification
    {
        /// <summary>
        /// Subscribe to change notifications from the server
        /// </summary>
        IFilesChanges Changes(string filesystem = null);

        /// <summary>
        /// Gets the shared operations headers.
        /// </summary>
        /// <value>The shared operations headers.</value>
        NameValueCollection SharedOperationsHeaders { get; }

        /// <summary>
        /// Get the <see cref="HttpJsonRequestFactory"/> for this store
        /// </summary>
        HttpJsonRequestFactory JsonRequestFactory { get; }

        /// <summary>
        /// Gets or sets the identifier for this store.
        /// </summary>
        /// <value>The identifier.</value>
        string Identifier { get; set; }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        /// <returns></returns>
        IFilesStore Initialize();

        /// <summary>
        /// Gets the async file system commands.
        /// </summary>
        /// <value>The async file system commands.</value>
        IAsyncFilesCommands AsyncFilesCommands { get; }

        /// <summary>
        /// Opens the async session.
        /// </summary>
        /// <returns></returns>
        IAsyncFilesSession OpenAsyncSession();

        /// <summary>
        /// Opens the async session.
        /// </summary>
        /// <returns></returns>
        IAsyncFilesSession OpenAsyncSession(string filesystem);

        /// <summary>
        /// Opens the async session.
        /// </summary>
        /// <returns></returns>
        IAsyncFilesSession OpenAsyncSession(OpenFilesSessionOptions sessionOptions);

        /// <summary>
        /// Gets the conventions.
        /// </summary>
        /// <value>The conventions.</value>
        FilesConvention Conventions { get; }

        /// <summary>
        /// Gets the URL.
        /// </summary>
        string Url { get; }


        FilesSessionListeners Listeners { get; }

        void SetListeners(FilesSessionListeners listeners);

    }
}
