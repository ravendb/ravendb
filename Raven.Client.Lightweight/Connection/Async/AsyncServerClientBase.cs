using System.Net.Http;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;
using System;
using System.Collections.Specialized;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Connection.Async
{
    public abstract class AsyncServerClientBase<TConvention, TReplicationInformer> : IDisposalNotification 
        where TConvention : Convention
        where TReplicationInformer : IReplicationInformerBase
    {
        protected AsyncServerClientBase(string serverUrl, TConvention convention, OperationCredentials credentials, HttpJsonRequestFactory jsonRequestFactory, 
                                     Guid? sessionId, NameValueCollection operationsHeaders)
        {
            WasDisposed = false;

            ServerUrl = serverUrl.TrimEnd('/'); 
            Conventions = convention;
            CredentialsThatShouldBeUsedOnlyInOperationsWithoutReplication = credentials;
            RequestFactory = jsonRequestFactory;
            SessionId = sessionId;
            
            OperationsHeaders = operationsHeaders ?? new NameValueCollection();

	        replicationInformer = new Lazy<TReplicationInformer>(GetReplicationInformer, true);
            readStrippingBase = new Lazy<int>(() => ReplicationInformer.GetReadStripingBase(true), true);

            MaxQuerySizeForGetRequest = 8 * 1024;
        }

        public int MaxQuerySizeForGetRequest
        {
            get;
            set;
        }

        public string ServerUrl
        {
            get; protected set;
        }

        public TConvention Conventions
        {
            get;
            private set;
        }

        protected Guid? SessionId
        {
            get;
            private set;
        }

        public HttpJsonRequestFactory RequestFactory
        {
            get;
            private set;
        }

        protected OperationCredentials CredentialsThatShouldBeUsedOnlyInOperationsWithoutReplication
        {
            get; set;
        }

        public virtual OperationCredentials PrimaryCredentials
        {
            get { return CredentialsThatShouldBeUsedOnlyInOperationsWithoutReplication; }
        }

        public NameValueCollection OperationsHeaders
        {
            get;
            set;
        }

        protected abstract string BaseUrl
        {
            get;
        }

        public abstract string UrlFor(string fileSystem);

        #region Execute with replication

        protected abstract TReplicationInformer GetReplicationInformer();


        private readonly Lazy<TReplicationInformer> replicationInformer;

        /// <summary>
        /// Allow access to the replication informer used to determine how we replicate requests
        /// </summary>
        public TReplicationInformer ReplicationInformer { get { return replicationInformer.Value; } }

        private readonly Lazy<int> readStrippingBase;
        private int requestCount;
        private volatile bool currentlyExecuting;

        internal async Task<T> ExecuteWithReplication<T>(HttpMethod method, Func<OperationMetadata, Task<T>> operation)
        {
            var currentRequest = Interlocked.Increment(ref requestCount);
            if (currentlyExecuting && Conventions.AllowMultipuleAsyncOperations == false)
                throw new InvalidOperationException("Only a single concurrent async request is allowed per async client instance.");

            currentlyExecuting = true;
            try
            {
                return await ReplicationInformer.ExecuteWithReplicationAsync(method, BaseUrl, CredentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, null, currentRequest, readStrippingBase.Value, operation)
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

        internal Task ExecuteWithReplication(HttpMethod method, Func<OperationMetadata, Task> operation)
        {
			// Convert the Func<string, Task> to a Func<string, Task<object>>
			return ExecuteWithReplication(method, u => operation(u).ContinueWith<object>(t =>
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
