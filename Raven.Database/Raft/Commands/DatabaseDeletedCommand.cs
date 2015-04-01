// -----------------------------------------------------------------------
//  <copyright file="DatabaseDocumentDeletedCommand.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;

using Rachis.Commands;

namespace Raven.Database.Raft.Commands
{
	public class DatabaseDeletedCommand : Command
	{
		public string Name { get; set; }

		public bool HardDelete { get; set; }

		public static DatabaseDeletedCommand Create(string databaseName, bool hardDelete)
		{
			return new DatabaseDeletedCommand
				   {
					   Name = databaseName,
					   HardDelete = hardDelete,
					   Completion = new TaskCompletionSource<object>()
				   };
		}
	}
}