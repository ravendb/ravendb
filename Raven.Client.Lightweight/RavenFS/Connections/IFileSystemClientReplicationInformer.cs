// -----------------------------------------------------------------------
//  <copyright file="IFileSystemClientReplicationInformer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client.Connection;

namespace Raven.Client.RavenFS.Connections
{
	public interface IFileSystemClientReplicationInformer : IReplicationInformerBase<RavenFileSystemClient>
	{
		 
	}
}