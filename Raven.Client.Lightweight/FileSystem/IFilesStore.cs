using Raven.Client.Connection;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
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
        /// Setup the WebRequest timeout for the session
        /// </summary>
        /// <param name="timeout">Specify the timeout duration</param>
        /// <remarks>
        /// Sets the timeout for the JsonRequest.  Scoped to the Current Thread.
        /// </remarks>
        IDisposable SetRequestsTimeoutFor(TimeSpan timeout);

        /// <summary>
        /// Gets the shared operations headers.
        /// </summary>
        /// <value>The shared operations headers.</value>
#if !NETFX_CORE
        NameValueCollection SharedOperationsHeaders { get; }
#else
		IDictionary<string,string> SharedOperationsHeaders { get; }
#endif

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

#if !NETFX_CORE

        /// <summary>
        /// Opens the session.
        /// </summary>
        /// <returns></returns>
        IFilesSession OpenSession();

        /// <summary>
        /// Opens the session for a particular filesystem
        /// </summary>
        IFilesSession OpenSession(string filesystem);

        /// <summary>
        /// Opens the session with the specified options.
        /// </summary>
        IFilesSession OpenSession(OpenFilesSessionOptions sessionOptions);

        /// <summary>
        /// Gets the filesystem commands.
        /// </summary>
        /// <value>The filesystem commands.</value>
        IFilesCommands FileSystemCommands { get; }

#endif

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
