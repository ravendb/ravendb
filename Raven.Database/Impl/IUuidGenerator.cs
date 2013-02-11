//-----------------------------------------------------------------------
// <copyright file="IUuidGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Raven.Abstractions.Data;

namespace Raven.Database.Impl
{
	public interface IUuidGenerator
	{
		Etag CreateSequentialUuid(UuidType type);
	}
}
