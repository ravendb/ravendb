//-----------------------------------------------------------------------
// <copyright file="General.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Database.Impl;
using Raven.Database.Impl.DTC;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Storage.Esent.StorageActions
{
	[CLSCompliant(false)]
	public partial class DocumentStorageActions : IDisposable, IGeneralStorageActions
	{
		public event Action OnStorageCommit = delegate { };
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
			TransactionalStorage transactionalStorage)
		{
			this.tableColumnsCache = tableColumnsCache;
			this.documentCodecs = documentCodecs;
			this.uuidGenerator = uuidGenerator;
			this.cacher = cacher;
			this.transactionalStorage = transactionalStorage;
			this.transactionContext = transactionContext;

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
			    logger.WarnException("Error when trying to open a new DocumentStorageActions", ex);
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
			if (lists != null)
				lists.Dispose();

			if (reduceKeysCounts != null)
				reduceKeysCounts.Dispose();

			if (reduceKeysStatus != null)
				reduceKeysStatus.Dispose();

			if (indexedDocumentsReferences != null)
				indexedDocumentsReferences.Dispose();

			if (reducedResults != null)
				reducedResults.Dispose();

			if (queue != null)
				queue.Dispose();

			if (directories != null)
				directories.Dispose();

			if (details != null)
				details.Dispose();

			if (identity != null)
				identity.Dispose();

			if (mappedResults != null)
				mappedResults.Dispose();

			if (scheduledReductions != null)
				scheduledReductions.Dispose();

			if (indexesStats != null)
				indexesStats.Dispose();

			if (files != null)
				files.Dispose();

			if (documents != null)
				documents.Dispose();

			if (tasks != null)
				tasks.Dispose();

			if (Equals(dbid, JET_DBID.Nil) == false && session != null)
				Api.JetCloseDatabase(session.JetSesid, dbid, CloseDatabaseGrbit.None);

		    if (sessionAndTransactionDisposer != null)
		        sessionAndTransactionDisposer();
		}

		public void UseLazyCommit()
		{
			UsingLazyCommit = true;
		}

		public void PulseTransaction()
		{
			transaction.Commit(CommitTransactionGrbit.LazyFlush);
			UseLazyCommit();
			transaction.Begin();
		}

		private int maybePulseCount;
		public void MaybePulseTransaction()
		{
			if (++maybePulseCount % 1000 != 0)
				return;

			var sizeInBytes = transactionalStorage.GetDatabaseTransactionVersionSizeInBytes();
			const int maxNumberOfCallsBeforePulsingIsForced = 50 * 1000;
			if (sizeInBytes <= 0) // there has been an error
			{
				if (maybePulseCount % maxNumberOfCallsBeforePulsingIsForced == 0)
					PulseTransaction();
				return;
			}
			var eightyPrecentOfMax = (transactionalStorage.MaxVerPagesValueInBytes*0.8);
			if (eightyPrecentOfMax <= sizeInBytes || maybePulseCount % maxNumberOfCallsBeforePulsingIsForced == 0)
				PulseTransaction();
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

		public long GetNextIdentityValue(string name)
		{
			Api.JetSetCurrentIndex(session, Identity, "by_key");
			Api.MakeKey(session, Identity, name, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Identity, SeekGrbit.SeekEQ) == false)
			{
				using (var update = new Update(session, Identity, JET_prep.Insert))
				{
					Api.SetColumn(session, Identity, tableColumnsCache.IdentityColumns["key"], name, Encoding.Unicode);
					Api.SetColumn(session, Identity, tableColumnsCache.IdentityColumns["val"], 1);

					update.Save();
				}
				return 1;
			}

			return Api.EscrowUpdate(session, Identity, tableColumnsCache.IdentityColumns["val"], 1) + 1;
		}

	}


}
