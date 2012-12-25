//-----------------------------------------------------------------------
// <copyright file="IUuidGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Database.Impl
{
	public interface IUuidGenerator
	{
		Guid CreateSequentialUuid();
	}
}