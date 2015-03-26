// -----------------------------------------------------------------------
//  <copyright file="ClusterConfigurationUpdateCommandHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Database.Raft.Commands;
using Raven.Database.Server.Tenancy;
using Raven.Json.Linq;

namespace Raven.Database.Raft.Storage.Handlers
{
	public class ClusterConfigurationUpdateCommandHandler : CommandHandler<ClusterConfigurationUpdateCommand>
	{
		public ClusterConfigurationUpdateCommandHandler(DocumentDatabase database, DatabasesLandlord landlord)
			: base(database, landlord)
		{
		}

		public override void Handle(ClusterConfigurationUpdateCommand command)
		{
			Database.Documents.Put(Constants.Cluster.ClusterConfigurationDocumentKey, null, RavenJObject.FromObject(command.Configuration), new RavenJObject(), null);
		}
	}
}