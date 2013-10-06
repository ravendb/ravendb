//-----------------------------------------------------------------------
// <copyright file="DeleteRemovedIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database.Indexing;

namespace Raven.Database.Plugins.Builtins
{
	public class DeleteRemovedIndexes : IStartupTask
	{
		#region IStartupTask Members

		public void Execute(DocumentDatabase database)
		{
			database.TransactionalStorage.Batch(actions =>
			{
			    actions.Lists.Read("Raven/Indexes/PendingDeletion", Etag.Empty, null, 100)
			           .Select(x => int.Parse(x.Key))
			           .Select(index => Task.Factory.StartNew(() => database.EnsureIndexDataIsDeleted(index)));
			});
		}

		#endregion
	}
}
