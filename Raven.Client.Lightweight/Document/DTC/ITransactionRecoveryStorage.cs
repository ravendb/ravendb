// -----------------------------------------------------------------------
//  <copyright file="ITransactionRecoveryStorage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Client.Document.DTC
{
	public interface ITransactionRecoveryStorage
	{
		ITransactionRecoveryStorageContext Create();
	}
}