// -----------------------------------------------------------------------
//  <copyright file="ConnectivityStatus.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Raft
{
	public enum ConnectivityStatus
	{
		Online,
		Offline,
		WrongCredentials
	}
}