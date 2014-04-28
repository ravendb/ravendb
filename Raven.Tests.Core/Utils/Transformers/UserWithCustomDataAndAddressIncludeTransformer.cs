// -----------------------------------------------------------------------
//  <copyright file="UsersWithCustomDataAndInclude.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;

namespace Raven.Tests.Core.Utils.Transformers
{
	public class UserWithCustomDataAndAddressIncludeTransformer : AbstractTransformerCreationTask<User>
	{
		public class Result
		{
			public string Name { get; set; }
			public string AddressId { get; set; }
			public string CustomData { get; set; }
		}

		public UserWithCustomDataAndAddressIncludeTransformer()
		{
			TransformResults = users => from user in users
										let _ = Include(user.AddressId)
										select new
										{
											user.Name,
											user.AddressId,
											CustomData = Query("customData")
										};
		}
	}
}