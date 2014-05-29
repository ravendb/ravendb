using Raven.Abstractions.Connection;
using Raven.Abstractions.RavenFS;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{
    public interface IFilesCommands
    {
        /// <summary>
        /// Gets or sets the operations headers
        /// </summary>
        /// <value>The operations headers.</value>
        NameValueCollection OperationsHeaders { get; set; }


        /// <summary>
        /// Primary credentials for access. Will be used also in replication context - for failovers
        /// </summary>
        OperationCredentials PrimaryCredentials { get; }


        /// <summary>
        /// Admin operations for current filesystem
        /// </summary>
        IFilesAdminCommands Admin { get; }

        /// <summary>
        /// Configuration operations for current filesystem
        /// </summary>
        IFilesConfigurationCommands Configuration { get; }

        /// <summary>
        /// Synchronization operations for current filesystem
        /// </summary>
        IFilesSynchronizationCommands Synchronization { get; }

        /// <summary>
        /// Storage operations for current filesystem
        /// </summary>
        IFilesStorageCommands Storage { get; }


        /// <summary>
        /// Returns a new <see cref="IFilesCommands"/> using the specified credentials
        /// </summary>
        /// <param name="credentialsForSession">The credentials for session.</param>
        IFilesCommands With(ICredentials credentialsForSession);

        /// <summary>
        /// Create a new instance of <see cref="IFilesCommands"/> that will interacts
        /// with the specified file system
        /// </summary>
        IFilesCommands ForFileSystem(string filesystem);

        

        IEnumerable<RemoteDirectory> GetDirectories(string path);
        IEnumerable<RemoteDirectory> GetDirectories(RemoteDirectory path);

        IEnumerable<RemoteFile> GetFiles(string path);
        IEnumerable<RemoteFile> GetFiles(RemoteDirectory path);


        /// <summary>
        /// Retrieve the statistics for the filesystem
        /// </summary>
        FileSystemStats GetStatistics();

        /// <summary>
        /// Disable all caching within the given scope
        /// </summary>
        IDisposable DisableAllCaching();

        /// <summary>
        /// Force the database commands to read directly from the master, unless there has been a failover.
        /// </summary>
        IDisposable ForceReadFromMaster();

    }

    public interface IFilesAdminCommands
    {
        /// <summary>
        /// Disables all replication
        /// </summary>
        void StopReplication();

        /// <summary>
        /// Enables replication
        /// </summary>
        void StartReplication();

        /// <summary>
        /// Get the replication status
        /// </summary>
        string GetReplicationStatus();
    }

    public interface IFilesConfigurationCommands
    {

    }

    public interface IFilesSynchronizationCommands
    {

    }

    public interface IFilesStorageCommands
    {

    }

}
