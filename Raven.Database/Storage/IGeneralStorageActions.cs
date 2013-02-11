//-----------------------------------------------------------------------
// <copyright file="IGeneralStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Database.Storage
{
	public interface IGeneralStorageActions
	{
		long GetNextIdentityValue(string name);
		void SetIdentityValue(string name, long value);

		void PulseTransaction();
		void MaybePulseTransaction();
	}
}
