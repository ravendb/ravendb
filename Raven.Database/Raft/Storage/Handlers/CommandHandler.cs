// -----------------------------------------------------------------------
//  <copyright file="CommandHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Rachis.Commands;

using Raven.Database.Server.Tenancy;

namespace Raven.Database.Raft.Storage.Handlers
{
	public abstract class CommandHandler<TCommand> : CommandHandler
		where TCommand : Command
	{
		protected CommandHandler(DocumentDatabase database, DatabasesLandlord landlord)
			: base(database, landlord)
		{
		}

		public Type HandledCommandType
		{
			get
			{
				return typeof(TCommand);
			}
		}

		public abstract void Handle(TCommand command);

		public override void Handle(object command)
		{
			Handle((TCommand)command);
		}
	}

	public abstract class CommandHandler
	{
		protected DocumentDatabase Database { get; private set; }

		protected DatabasesLandlord Landlord { get; private set; }

		protected CommandHandler(DocumentDatabase database, DatabasesLandlord landlord)
		{
			Database = database;
			Landlord = landlord;
		}

		public abstract void Handle(object command);
	}
}