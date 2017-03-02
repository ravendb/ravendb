// -----------------------------------------------------------------------
//  <copyright file="DatabaseUpdateCommand.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;

using Rachis.Commands;

using Raven.Abstractions.Data;
using Raven.Database.Util;

namespace Raven.Database.Raft.Commands
{
    public class DatabaseUpdateCommand : Command
    {
        public DatabaseDocument Document { get; set; }

        public static DatabaseUpdateCommand Create(string databaseName, DatabaseDocument document)
        {
            document.Id = DatabaseHelper.GetDatabaseName(databaseName);

            return new DatabaseUpdateCommand
            {
                Document = document,
                Completion = new TaskCompletionSource<object>()
            };
        }
    }
}
