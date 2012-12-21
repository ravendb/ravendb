//-----------------------------------------------------------------------
// <copyright file="AbstractPutTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public abstract class AbstractPutTrigger: IRequiresDocumentDatabaseInitialization
	{
		/// <summary>
		///  Ask the trigger whatever the PUT should be vetoed.
		///  If the trigger vote to veto the PUT, it needs to provide a human readable 
		///  explanation why the PUT was rejected.
		///  </summary>
		/// <remarks>
		///  This method SHOULD NOT modify either the document or the metadata.
		///  </remarks>
		/// <param name="key">The document key</param>
		/// <param name="document">The new document about to be put into Raven</param>
		/// <param name="metadata">The new document metadata</param>
		/// <param name="transactionInformation">The current transaction, if it exists</param>
		/// <returns>Whatever the put was vetoed or not</returns>
		public virtual VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			return VetoResult.Allowed;
		}

		/// <summary>
		///  Allow the trigger to perform any logic just before the document is saved to disk.
		///  Any modifications the trigger makes to the document or the metadata will be persisted 
		///  to disk.
		///  </summary><remarks>
		///  If the trigger need to access the previous state of the document, the trigger should
		///  implement <seealso cref="IRequiresDocumentDatabaseInitialization" /> and use the provided
		///  <seealso cref="DocumentDatabase" /> instance to Get it. The returned result would be the old
		///  document (if it exists) or null.
		///  Any call to the provided <seealso cref="DocumentDatabase" /> instance will be done under the
		///  same transaction as the PUT operation.
		///  </remarks><param name="key">The document key</param><param name="document">The new document about to be put into Raven</param><param name="metadata">The new document metadata</param><param name="transactionInformation">The current transaction, if it exists</param>
		public virtual void OnPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			
		}

		/// <summary>
		///  Allow the trigger to perform any logic after the document was put but still in the 
		///  same transaction as the put
		///  </summary><remarks>
		///  Any call to the provided <seealso cref="DocumentDatabase" /> instance will be done under the
		///  same transaction as the PUT operation.
		///  </remarks><param name="key">The document key</param><param name="document">The new document about to be put into Raven</param><param name="metadata">The new document metadata</param>
		/// <param name="etag">The etag of the just put document</param>
		/// <param name="transactionInformation">The current transaction, if it exists</param>
		public virtual void AfterPut(string key, RavenJObject document, RavenJObject metadata, Guid etag, TransactionInformation transactionInformation)
		{
			
		}

		/// <summary>
		///  Allow the trigger to perform any logic _after_ the transaction was committed.
		///  For example, by notifying interested parties.
		///  </summary><remarks>
		///  This method SHOULD NOT modify either the document or the metadata
		///  </remarks><param name="key">The document key</param><param name="document">The document that was put into Raven</param><param name="metadata">The document metadata</param>
		/// <param name="etag">The etag of the just put document</param>
		public virtual void AfterCommit(string key, RavenJObject document, RavenJObject metadata, Guid etag)
		{
			
		}

		public void Initialize(DocumentDatabase database)
		{
			Database = database;
			Initialize();
		}

		public virtual void Initialize()
		{

		}

		public virtual void SecondStageInit()
		{

		}

		public DocumentDatabase Database { get; set; }
	}
}
