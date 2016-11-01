using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Extensions;
using Raven.Client.FileSystem.Changes;
using Raven.Client.FileSystem.Connection;
using Raven.Client.FileSystem.Extensions;
using Raven.Client.Util;

namespace Raven.Client.FileSystem
{
    public class FilesStore : IFilesStore
    {
        /// <summary>
        /// The current session id - only used during construction
        /// </summary>
        [ThreadStatic]
        private static Guid? currentSessionId;

        private HttpJsonRequestFactory jsonRequestFactory;
        private FilesConvention conventions;
        private readonly AtomicDictionary<IFilesChanges> fileSystemChanges = new AtomicDictionary<IFilesChanges>(StringComparer.OrdinalIgnoreCase);
        private readonly AtomicDictionary<IAsyncFilesCommandsImpl> fileSystemCommands = new AtomicDictionary<IAsyncFilesCommandsImpl>(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, IFilesReplicationInformer> replicationInformers = new ConcurrentDictionary<string, IFilesReplicationInformer>(StringComparer.OrdinalIgnoreCase);

        private bool initialized;
        private FilesSessionListeners listeners = new FilesSessionListeners();

        private const int DefaultNumberOfCachedRequests = 2048;
        private int maxNumberOfCachedRequests = DefaultNumberOfCachedRequests;

        public FilesStore()
        {
            SharedOperationsHeaders = new NameValueCollection();
            Conventions = new FilesConvention();
        }

        /// <summary>
        /// Gets or sets the credentials.
        /// </summary>
        /// <value>The credentials.</value>
        public ICredentials Credentials 
        {
            get { return credentials; }
            set
            {
                credentials = value ?? CredentialCache.DefaultNetworkCredentials;
            }
        }
        private ICredentials credentials;

        /// <summary>
        /// The API Key to use when authenticating against a RavenDB server that
        /// supports API Key authentication
        /// </summary>
        public string ApiKey { get; set; }

        public IFilesChanges Changes(string filesystem = null)
        {
            AssertInitialized();

            if (string.IsNullOrWhiteSpace(filesystem))
                filesystem = DefaultFileSystem;

            return fileSystemChanges.GetOrAdd(filesystem, CreateFileSystemChanges );
        }
        
        protected virtual IFilesChanges CreateFileSystemChanges(string filesystem)
        {
            if (string.IsNullOrEmpty(Url))
                throw new InvalidOperationException("Changes API requires usage of server/client");

            var tenantUrl = Url + "/fs/" + filesystem;

            var commands = fileSystemCommands.GetOrAdd(filesystem, x => (IAsyncFilesCommandsImpl)AsyncFilesCommands.ForFileSystem(x));

            using (NoSynchronizationContext.Scope())
            {
                var client = new FilesChangesClient(tenantUrl,
                    ApiKey,
                    Credentials,
                    jsonRequestFactory,
                    Conventions,
                    ((AsyncFilesServerClient) AsyncFilesCommands).TryResolveConflictByUsingRegisteredListenersAsync,
                    () =>
                    {
                        fileSystemChanges.Remove(filesystem);
                        fileSystemCommands.Remove(filesystem);
                    });

                return client;
            }
        }

        /// <summary>
        /// Gets the shared operations headers.
        /// </summary>
        /// <value>The shared operations headers.</value>
        public virtual NameValueCollection SharedOperationsHeaders { get; protected set; }

        /// <summary>
        /// Whatever this instance has json request factory available
        /// </summary>
        public virtual bool HasJsonRequestFactory
        {
            get { return true; }
        }

        ///<summary>
        /// Get the <see cref="HttpJsonRequestFactory"/> for the stores
        ///</summary>
        public virtual HttpJsonRequestFactory JsonRequestFactory
        {
            get
            {
                return jsonRequestFactory;
            }
        }

        public string DefaultFileSystem
        {
            get { return defaultFileSystem; }
            set
            { 
                if (string.IsNullOrEmpty(value))
                    throw new ArgumentException("DefaultFileSystem can not be null or empty");
                defaultFileSystem = value;
            }
        }

        private string defaultFileSystem;

        private bool disableReplicationInformerGeneration = false;
        public IFilesReplicationInformer GetReplicationInformerForFileSystem(string fsName = null)
        {
            if (disableReplicationInformerGeneration)
                return null;

            var key = Url;
            fsName = fsName ?? DefaultFileSystem;
            if (string.IsNullOrEmpty(fsName) == false)
            {
                key = MultiDatabase.GetRootFileSystemUrl(Url) + "/fs/" + fsName;
            }

            var result = replicationInformers.GetOrAdd(key, replicationUrl => Conventions.ReplicationInformerFactory(replicationUrl, jsonRequestFactory));
            return result;
        }

        /// <summary>
        /// Gets the conventions.
        /// </summary>
        /// <value>The conventions.</value>
        public virtual FilesConvention Conventions
        {
            get { return conventions ?? (conventions = new FilesConvention()); }
            set { conventions = value; }
        }

        /// <summary>
        /// Max number of cached requests (default: 2048)
        /// </summary>
        public int MaxNumberOfCachedRequests
        {
            get { return maxNumberOfCachedRequests; }
            set
            {
                maxNumberOfCachedRequests = value;
                jsonRequestFactory.ResetCache(maxNumberOfCachedRequests);
            }
        }

        public Func<HttpMessageHandler> HttpMessageHandlerFactory { get; set; }

        private string url;

        /// <summary>
        /// Gets or sets the URL.
        /// </summary>
        public virtual string Url
        {
            get { return url; }
            set
            {
                if(value == null)
                    throw new ArgumentNullException("value");
                url = value.TrimEnd('/');
            }
        }
        

        /// <summary>
        /// Gets or sets the identifier for this store.
        /// </summary>
        /// <value>The identifier.</value>
        public virtual string Identifier
        {
            get
            {
                if (identifier != null)
                    return identifier;
                if (Url == null)
                    return null;
                return Url;
            }
            set { identifier = value; }
        }
        private string identifier;


        public IFilesStore Initialize()
        {
            return Initialize(true, false);
        }

        public IFilesStore Initialize(bool ensureFileSystemExists = true, bool failIfCannotCreate = true)
        {
            if (initialized)
                return this;
            disableReplicationInformerGeneration = true;
            jsonRequestFactory = new HttpJsonRequestFactory(MaxNumberOfCachedRequests, HttpMessageHandlerFactory, authenticationScheme: conventions.AuthenticationScheme);

            try
            {
                SecurityExtensions.InitializeSecurity(Conventions, JsonRequestFactory, Url);

                InitializeInternal();

                initialized = true;

                if (ensureFileSystemExists && string.IsNullOrEmpty(DefaultFileSystem) == false)
                {
                    try
                    {

                        AsyncFilesCommands.ForFileSystem(DefaultFileSystem).EnsureFileSystemExistsAsync().GetAwaiter().GetResult();

                    }
                    catch (Exception)
                    {
                        if (failIfCannotCreate)
                            throw;
                    }
                }
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
            finally
            {
                disableReplicationInformerGeneration = false;
            }

            return this;
        }

        protected virtual void InitializeInternal()
        {
            AsyncFilesCommandsGenerator = () => 
                new AsyncFilesServerClient(Url, 
                    DefaultFileSystem, 
                    Conventions, 
                    new OperationCredentials(ApiKey, Credentials),
                    jsonRequestFactory,
                    currentSessionId,
                    GetReplicationInformerForFileSystem,
                    Listeners.ConflictListeners);
        }

        /// <summary>
        /// Generate new instance of files commands
        /// </summary>
        protected Func<IAsyncFilesCommands> AsyncFilesCommandsGenerator;
        
        /// <summary>
        /// Gets the async file system commands.
        /// </summary>
        /// <value>The async file system commands.</value>
        public virtual IAsyncFilesCommands AsyncFilesCommands
        {
            get
            {
                AssertInitialized();
                var commands = AsyncFilesCommandsGenerator();
                foreach (string key in SharedOperationsHeaders)
                {
                    var values = SharedOperationsHeaders.GetValues(key);
                    if (values == null)
                        continue;
                    foreach (var value in values)
                    {
                        commands.OperationsHeaders[key] = value;
                    }
                }
                return commands;
            }
        }

        public virtual IAsyncFilesSession OpenAsyncSession()
        {
            return OpenAsyncSession(
                        new OpenFilesSessionOptions
                        {
                            FileSystem = this.DefaultFileSystem
                        });
        }

        public virtual IAsyncFilesSession OpenAsyncSession(string filesystem)
        {
            if (string.IsNullOrWhiteSpace(filesystem))
                throw new ArgumentException("Filesystem cannot be null, empty or whitespace.", "filesystem");

            return OpenAsyncSession(
                    new OpenFilesSessionOptions
                    {
                        FileSystem = filesystem
                    });
        }

        public IAsyncFilesSession OpenAsyncSession(OpenFilesSessionOptions sessionOptions)
        {
            AssertInitialized();
            EnsureNotClosed();

            if (string.IsNullOrWhiteSpace(sessionOptions.FileSystem))
                throw new ArgumentException("Filesystem cannot be null, empty or whitespace.", "FileSystem");


            var sessionId = Guid.NewGuid();
            currentSessionId = sessionId;
            try
            {
                var client = SetupCommandsAsync(this.AsyncFilesCommands, sessionOptions);
                var session = new AsyncFilesSession(this, client, this.Listeners, sessionId);
                AfterSessionCreated(session);
                return session;
            }
            finally
            {
                currentSessionId = null;
            }
        }

        private static IAsyncFilesCommands SetupCommandsAsync(IAsyncFilesCommands filesCommands, OpenFilesSessionOptions options)
        {
            if (string.IsNullOrWhiteSpace(options.FileSystem))
                throw new ArgumentException("Filesystem cannot be null, empty or whitespace.", "FileSystem");

            filesCommands = filesCommands.ForFileSystem(options.FileSystem);
            if (options.ApiKey != null || options.Credentials != null)
                filesCommands = filesCommands.With(new OperationCredentials(options.ApiKey, options.Credentials));
            
            return filesCommands;
        }

        public FilesSessionListeners Listeners
        {
            get  { return listeners; }
        }
        public void SetListeners(FilesSessionListeners newListeners)
        {
            this.listeners = newListeners;         
        }

        private string _connectionStringName;

        public string ConnectionStringName
        {
            get { return _connectionStringName; }
            set
            {
                _connectionStringName = value;
                HandleConnectionStringOptions();
            }
        }

        private void HandleConnectionStringOptions()
        {
            if (!String.IsNullOrWhiteSpace(ConnectionStringName))
            {
                var parser = ConnectionStringParser<FilesConnectionStringOptions>.FromConnectionStringName(ConnectionStringName);
                parser.Parse();

                var options = parser.ConnectionStringOptions;
                if (options.Credentials != null)
                    Credentials = options.Credentials;
                if (string.IsNullOrEmpty(options.Url) == false)
                    Url = options.Url;
                if (string.IsNullOrEmpty(options.DefaultFileSystem) == false)
                    DefaultFileSystem = options.DefaultFileSystem;
                if (string.IsNullOrEmpty(options.ApiKey) == false)
                    ApiKey = options.ApiKey;
            }
        }

        protected void EnsureNotClosed()
        {
            if (WasDisposed)
                throw new ObjectDisposedException(GetType().Name, "The files store has already been disposed and cannot be used");
        }

        protected void AssertInitialized()
        {
            if (!initialized)
                throw new InvalidOperationException("You cannot open a session or access the files commands before initializing the files store. Did you forget calling Initialize()?");
        }

        ///<summary>
        /// Internal notification for integration tools, mainly
        ///</summary>
        public event Action<InMemoryFilesSessionOperations> SessionCreatedInternal = x => { };

        protected virtual void AfterSessionCreated(InMemoryFilesSessionOperations session)
        {
            SessionCreatedInternal(session);
        }

        public event EventHandler AfterDispose = (obj, sender) => { };

        public bool WasDisposed
        {
            get;
            private set;
        }

        public void Dispose()
        {
#if DEBUG
            GC.SuppressFinalize(this);
#endif

            var tasks = new List<Task>();
            foreach (var fileSystemChange in fileSystemChanges)
            {
                var remoteFileSystemChanges = fileSystemChange.Value as FilesChangesClient;
                if (remoteFileSystemChanges != null)
                {
                    tasks.Add(remoteFileSystemChanges.DisposeAsync());
                }
                else
                {
                    using (fileSystemChange.Value as IDisposable) { }
                }
            }

            foreach (var fileSystemCommand in fileSystemCommands)
            {
                var remoteFileSystemCommand = fileSystemCommand.Value as IDisposable;
                if (remoteFileSystemCommand != null)
                    remoteFileSystemCommand.Dispose();
            }

            foreach (var replicationInformer in replicationInformers)
            {
                replicationInformer.Value.Dispose();
            }

            // try to wait until all the async disposables are completed
            Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(5));

            // if this is still going, we continue with disposal, it is for grace only, anyway
            if (jsonRequestFactory != null)
                jsonRequestFactory.Dispose();

            WasDisposed = true;
            AfterDispose(this, EventArgs.Empty);
        }
    }
}
