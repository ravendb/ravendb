//-----------------------------------------------------------------------
// <copyright file="From30To32.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Extensions;
using Raven.Database.Impl;

namespace Raven.Storage.Esent.SchemaUpdates
{
	public class From32To33 : ISchemaUpdate
	{
		public string FromSchemaVersion
		{
			get { return "3.2"; }
		}

		public void Init(IUuidGenerator generator)
		{
		}

		public void Update(Session session, JET_DBID dbid)
		{
			Transaction tx;
			using (tx = new Transaction(session))
			{
				using (var tbl = new Table(session, dbid, "indexes_stats", OpenTableGrbit.PermitDDL | OpenTableGrbit.DenyRead|OpenTableGrbit.DenyWrite))
				{
					JET_COLUMNID columnid;
					var defaultValue = BitConverter.GetBytes(0);
					Api.JetAddColumn(session, tbl, "reduce_attempts", new JET_COLUMNDEF
					{
						coltyp = JET_coltyp.Long,
						grbit =
							ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnEscrowUpdate | ColumndefGrbit.ColumnNotNULL
					}, defaultValue, defaultValue.Length, out columnid);

					Api.JetAddColumn(session, tbl, "reduce_errors", new JET_COLUMNDEF
					{
						coltyp = JET_coltyp.Long,
						grbit =
							ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnEscrowUpdate | ColumndefGrbit.ColumnNotNULL
					}, defaultValue, defaultValue.Length, out columnid);

					Api.JetAddColumn(session, tbl, "reduce_successes", new JET_COLUMNDEF
					{
						coltyp = JET_coltyp.Long,
						grbit =
							ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnEscrowUpdate
					}, defaultValue, defaultValue.Length, out columnid);
				}
				tx.Commit(CommitTransactionGrbit.LazyFlush);
				tx.Dispose();
				tx = new Transaction(session);
			}

			using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
			{
				Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
				var columnids = Api.GetColumnDictionary(session, details);

				using (var update = new Update(session, details, JET_prep.Replace))
				{
					Api.SetColumn(session, details, columnids["schema_version"], "3.3", Encoding.Unicode);

					update.Save();
				}
			}
			tx.Commit(CommitTransactionGrbit.None);
			tx.Dispose();
		}

	}
}