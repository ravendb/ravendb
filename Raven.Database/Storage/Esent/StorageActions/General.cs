//-----------------------------------------------------------------------
// <copyright file="General.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Database.Impl;
using Raven.Database.Impl.DTC;
using Raven.Database.Plugins;
using Raven.Storage.Esent;

namespace Raven.Database.Storage.Esent.StorageActions
{
    [CLSCompliant(false)]
    public partial class DocumentStorageActions : IDisposable, IGeneralStorageActions
    {
        public event Action OnStorageCommit = delegate { };
        public event Action BeforeStorageCommit;
        public event Action AfterStorageCommit;

        private readonly object maybePulseLock = new object();
        private readonly TableColumnsCache tableColumnsCache;
        private readonly OrderedPartCollection<AbstractDocumentCodec> documentCodecs;
        private readonly IUuidGenerator uuidGenerator;
        private readonly IDocumentCacher cacher;
        private readonly TransactionalStorage transactionalStorage;
        protected readonly JET_DBID dbid;

        protected static readonly ILog logger = LogManager.GetCurrentClassLogger();
        protected readonly Session session;
        private Transaction transaction;
        private readonly Dictionary<Etag, Etag> etagTouches = new Dictionary<Etag, Etag>();
        private readonly EsentTransactionContext transactionContext;
        private readonly Action sessionAndTransactionDisposer;

        public JET_DBID Dbid
        {
            get { return dbid; }
        }

        public Session Session
        {
            get { return session; }
        }

        [CLSCompliant(false)]
        [DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
        public DocumentStorageActions(
            JET_INSTANCE instance,
            string database,
            TableColumnsCache tableColumnsCache,
            OrderedPartCollection<AbstractDocumentCodec> documentCodecs,
            IUuidGenerator uuidGenerator,
            IDocumentCacher cacher,
            EsentTransactionContext transactionContext,
            TransactionalStorage transactionalStorage
            )
        {
            this.tableColumnsCache = tableColumnsCache;
            this.documentCodecs = documentCodecs;
            this.uuidGenerator = uuidGenerator;
            this.cacher = cacher;
            this.transactionalStorage = transactionalStorage;
            this.transactionContext = transactionContext;
            scheduledReductionsPerViewAndLevel = transactionalStorage.GetScheduledReductionsPerViewAndLevel();
            try
            {
                if (transactionContext == null)
                {
                    session = new Session(instance);
                    transaction = new Transaction(session);
                    sessionAndTransactionDisposer = () =>
                    {
                        if(transaction != null)
                            transaction.Dispose();
                        if(session != null)
                            session.Dispose();
                    };
                }
                else
                {
                    session = transactionContext.Session;
                    transaction = transactionContext.Transaction;
                    var disposable = transactionContext.EnterSessionContext();
                    sessionAndTransactionDisposer = disposable.Dispose;
                }
                Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.None);
            }
            catch (Exception ex)
            {
                string location;
                try
                {
                    location = new StackTrace(true).ToString();
                }
                catch (Exception)
                {
                    location = "cannot get stack trace";
                }
                logger.WarnException("Error when trying to open a new DocumentStorageActions from \r\n" + location, ex);
                try
                {
                    Dispose();
                }
                catch (Exception e)
                {
                    logger.WarnException("Error on dispose when the ctor threw an exception, resources may have leaked", e);
                }
                throw;
            }
        }

        [DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
        public void Dispose()
        {
            var toDispose = new[]
                            {
                                documents, 
                                queue, 
                                lists, 
                                directories, 
                                files, 
                                indexesStats, 
                                indexesStatsReduce, 
                                indexesEtags, 
                                scheduledReductions, 
                                mappedResults, 
                                reducedResults, 
                                tasks, 
                                identity, 
                                details, 
                                reduceKeysCounts, 
                                reduceKeysStatus, 
                                indexedDocumentsReferences
                            };

            var aggregator = new ExceptionAggregator("DocumentStorageActions disposal error.");

            foreach (var dispose in toDispose)
            {
                if (dispose == null)
                    continue;

                aggregator.Execute(() => dispose.Dispose());
            }

            aggregator.Execute(() =>
            {
            if (Equals(dbid, JET_DBID.Nil) == false && session != null)
                Api.JetCloseDatabase(session.JetSesid, dbid, CloseDatabaseGrbit.None);
            });

            aggregator.Execute(() =>
            {
            if (sessionAndTransactionDisposer != null)
                sessionAndTransactionDisposer();
            });

            aggregator.ThrowIfNeeded();
        }

        internal void ExecuteOnStorageCommit()
        {
            if (OnStorageCommit != null)
            {
                OnStorageCommit();
            }
        }

        internal void ExecuteBeforeStorageCommit()
        {
            var before = BeforeStorageCommit;
            if (before != null)
            {
                before();
            }
        }

        internal void ExecuteAfterStorageCommit()
        {
            var after = AfterStorageCommit;
            if (after != null)
            {
                after();
            }
        }

        public void UseLazyCommit()
        {
            UsingLazyCommit = true;
        }

        public void PulseTransaction()
        {
            try
            {
                ExecuteBeforeStorageCommit();

                transaction.Commit(CommitTransactionGrbit.LazyFlush);
                UseLazyCommit();
                transaction.Begin();
            }
            finally
            {
                ExecuteAfterStorageCommit();
            }
        }

        private int maybePulseCount;
        private int totalMaybePulseCount;
        public bool MaybePulseTransaction(int addToPulseCount = 1, Action beforePulseTransaction = null)
        {
            Interlocked.Add(ref totalMaybePulseCount, addToPulseCount);
            var increment = Interlocked.Add(ref maybePulseCount, addToPulseCount);
            if (increment < 1024)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref maybePulseCount, 0, increment) != increment)
            {
                return false;
            }

            lock (maybePulseLock)
            {
                var sizeInBytes = transactionalStorage.GetDatabaseTransactionVersionSizeInBytes();
                const int maxNumberOfCallsBeforePulsingIsForced = 50*1000;
                if (sizeInBytes <= 0) // there has been an error
                {
                    if (totalMaybePulseCount >= maxNumberOfCallsBeforePulsingIsForced)
                    {
                        Interlocked.Exchange(ref totalMaybePulseCount, 0);

                        if (beforePulseTransaction != null)
                            beforePulseTransaction();

                        if (logger.IsDebugEnabled)
                            logger.Debug("MaybePulseTransaction() --> PulseTransaction()");
                        PulseTransaction();
                    return true;
                }
                    return false;
                }

                var eightyPrecentOfMax = (transactionalStorage.MaxVerPagesValueInBytes*0.8);
                if (eightyPrecentOfMax <= sizeInBytes ||
                    totalMaybePulseCount >= maxNumberOfCallsBeforePulsingIsForced)
                {
                    Interlocked.Exchange(ref totalMaybePulseCount, 0);

                    if (beforePulseTransaction != null)
                        beforePulseTransaction();

                    if (logger.IsDebugEnabled)                        logger.Debug("MaybePulseTransaction() --> PulseTransaction()");
                    PulseTransaction();
                return true;
            }
                return false;
        }
        }

        public bool UsingLazyCommit { get; set; }

        public Action Commit(CommitTransactionGrbit txMode)
        {
            if (transactionContext == null)
            {
                transaction.Commit(txMode);
            }

            return OnStorageCommit;
        }


        public void SetIdentityValue(string name, long value)
        {
            Api.JetSetCurrentIndex(session, Identity, "by_key");
            Api.MakeKey(session, Identity, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            using (var update = new Update(session, Identity, Api.TrySeek(session, Identity, SeekGrbit.SeekEQ) ? JET_prep.Replace : JET_prep.Insert))
            {
                Api.SetColumn(session, Identity, tableColumnsCache.IdentityColumns["key"], name, Encoding.Unicode);
                Api.SetColumn(session, Identity, tableColumnsCache.IdentityColumns["val"], (int)value);

                update.Save();
            }
        }

        public long GetNextIdentityValue(string name, int val)
        {
            Api.JetSetCurrentIndex(session, Identity, "by_key");
            Api.MakeKey(session, Identity, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, Identity, SeekGrbit.SeekEQ) == false)
            {
                if (val == 0)
                    return 0;
            
                using (var update = new Update(session, Identity, JET_prep.Insert))
                {
                    Api.SetColumn(session, Identity, tableColumnsCache.IdentityColumns["key"], name, Encoding.Unicode);
                    Api.SetColumn(session, Identity, tableColumnsCache.IdentityColumns["val"], val);

                    update.Save();
                }
                return val;
            }



            return Api.EscrowUpdate(session, Identity, tableColumnsCache.IdentityColumns["val"], val) + val;
        }

        public IEnumerable<KeyValuePair<string, long>> GetIdentities(int start, int take, out long totalCount)
        {
            Api.JetSetCurrentIndex(session, Identity, "by_key");

            int numRecords;
            Api.JetIndexRecordCount(session, Identity, out numRecords, 0);

            totalCount = numRecords;
            if (totalCount <= 0 || Api.TryMoveFirst(session, Identity) == false || TryMoveTableRecords(Identity, start, backward: false))
                return Enumerable.Empty<KeyValuePair<string, long>>();

            var results = new List<KeyValuePair<string, long>>();

            do
            {
                var identityName = Api.RetrieveColumnAsString(session, Identity, tableColumnsCache.IdentityColumns["key"]);
                var identityValue = Api.RetrieveColumnAsInt32(session, Identity, tableColumnsCache.IdentityColumns["val"]);

                results.Add(new KeyValuePair<string, long>(identityName, identityValue.Value));
}
            while (Api.TryMoveNext(session, Identity) && results.Count < take);

            return results;
        }
    }
}
