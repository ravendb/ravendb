using System.Net.Http;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;
using System;
using System.Collections.Specialized;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Client.Metrics;

namespace Raven.Client.Connection.Async
{
    public abstract class AsyncServerClientBase<TConvention, TReplicationInformer> : IDisposalNotification
        where TConvention : ConventionBase, new()
        where TReplicationInformer : IReplicationInformerBase
    {
        private const int DefaultNumberOfCachedRequests = 2048;

        protected AsyncServerClientBase(string serverUrl, TConvention convention, OperationCredentials credentials, HttpJsonRequestFactory jsonRequestFactory,
                                     Guid? sessionId, NameValueCollection operationsHeaders, Func<string, TReplicationInformer> replicationInformerGetter, string resourceName)
        {
            WasDisposed = false;

            ServerUrl = serverUrl.TrimEnd('/');
            Conventions = convention ?? new TConvention();
            CredentialsThatShouldBeUsedOnlyInOperationsWithoutReplication = credentials;
            RequestFactory = jsonRequestFactory ?? new HttpJsonRequestFactory(DefaultNumberOfCachedRequests, authenticationScheme: Conventions.AuthenticationScheme);

            if (jsonRequestFactory == null)
                SecurityExtensions.InitializeSecurity(Conventions, RequestFactory, ServerUrl, autoRefreshToken: false);

            SessionId = sessionId;
            OperationsHeaders = operationsHeaders ?? DefaultNameValueCollection;

            ReplicationInformerGetter = replicationInformerGetter ?? DefaultReplicationInformerGetter();
            replicationInformer = new Lazy<TReplicationInformer>(() => ReplicationInformerGetter(resourceName), true);
            MaxQuerySizeForGetRequest = 8 * 1024;
        }

        protected abstract Func<string, TReplicationInformer> DefaultReplicationInformerGetter();

        public int MaxQuerySizeForGetRequest { get; set; }

        public string ServerUrl { get; private set; }

        public TConvention Conventions { get; private set; }

        protected Guid? SessionId { get; private set; }

        public HttpJsonRequestFactory RequestFactory { get; private set; }

        protected OperationCredentials CredentialsThatShouldBeUsedOnlyInOperationsWithoutReplication { get; set; }

        public virtual OperationCredentials PrimaryCredentials
        {
            get { return CredentialsThatShouldBeUsedOnlyInOperationsWithoutReplication; }
        }

        public NameValueCollection OperationsHeaders { get; set; }

        protected abstract string BaseUrl { get; }

        public abstract string UrlFor(string fileSystem);

        /// <summary>
        ///     Force the File ServerClient to read directly from the master, unless there has been a failover.
        /// </summary>
        public IDisposable ForceReadFromMaster()
        {
            var old = ReadStrippingBase;
            readStrippingBase = -1;// this means that will have to use the master url first
            return new DisposableAction(() => readStrippingBase = old);
        }

        #region Execute with replication

        //protected abstract TReplicationInformer GetReplicationInformer();


        private readonly Lazy<TReplicationInformer> replicationInformer;

        /// <summary>
        /// Allow access to the replication informer used to determine how we replicate requests
        /// </summary>
        public TReplicationInformer ReplicationInformer { get { return replicationInformer.Value; } }
        protected readonly Func<string, TReplicationInformer> ReplicationInformerGetter;

        private int? readStrippingBase = null;

        public int ReadStrippingBase
        {
            get
            {
                if (readStrippingBase.HasValue)
                {
                    return readStrippingBase.Value;
                }
                else if (ReplicationInformer == null)
                {
                    readStrippingBase = -1;
                }
                else readStrippingBase = ReplicationInformer.GetReadStripingBase(true);
                return readStrippingBase.Value;
            }
            internal set { readStrippingBase = value; }
        }
        private int requestCount;
        private volatile bool currentlyExecuting;
        private static readonly NameValueCollection DefaultNameValueCollection = new NameValueCollection();

        internal async Task<T> ExecuteWithReplication<T>(HttpMethod method, Func<OperationMetadata, IRequestTimeMetric, Task<T>> operation)
        {
            var currentRequest = Interlocked.Increment(ref requestCount);
            if (currentlyExecuting && Conventions.AllowMultipuleAsyncOperations == false)
                throw new InvalidOperationException("Only a single concurrent async request is allowed per async client instance.");

            currentlyExecuting = true;
            try
            {
                return await ReplicationInformer
                    .ExecuteWithReplicationAsync(method, BaseUrl, CredentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, currentRequest, ReadStrippingBase, operation)
                    .ConfigureAwait(false);
            }
            catch (AggregateException e)
            {
                var singleException = e.ExtractSingleInnerException();
                if (singleException != null)
                    throw singleException;

                throw;
            }
            finally
            {
                currentlyExecuting = false;
            }
        }

        internal Task ExecuteWithReplication(HttpMethod method, Func<OperationMetadata, IRequestTimeMetric, Task> operation)
        {
            // Convert the Func<string, Task> to a Func<string, Task<object>>
            return ExecuteWithReplication(method, (u, rtm) => operation(u, rtm).ContinueWith<object>(t =>
             {
                 t.AssertNotFailed();
                 return null;
             }));
        }

        #endregion

        #region IDisposalNotification

        public event EventHandler AfterDispose = (sender, args) => { };

        public bool WasDisposed { get; protected set; }

        public virtual void Dispose()
        {
            WasDisposed = true;
            AfterDispose(this, EventArgs.Empty);
        }

        #endregion
    }
}
