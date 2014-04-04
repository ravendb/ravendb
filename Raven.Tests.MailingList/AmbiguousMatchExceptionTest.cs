// -----------------------------------------------------------------------
//  <copyright file="AmbiguousMatchExceptionTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class AmbiguousMatchExceptionTest : NoDisposalNeeded
	{
		public class GeneralThing
		{
			public string Name { get; set; }
		}

		public class SpecificThing : GeneralThing
		{
			public string SpecificProperty { get; set; }
		}

		public class GeneralThingOwner
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public GeneralThing Thing { get; set; }
		}

		public class SpecificThingOwner : GeneralThingOwner
		{
			public new SpecificThing Thing { get; set; }
		}

		public class SpecificThingOwners_AndTheirThings : AbstractIndexCreationTask<SpecificThingOwner>
		{
			public SpecificThingOwners_AndTheirThings()
			{
				Map = specificThingOwners => specificThingOwners
											  .Select(owner => new
											  {
												  owner.Id,
												  owner.Name,
												  ThingName = owner.Thing.Name
											  });
				TransformResults = (database, docs) => docs
														.Select(owner => new
														{
															Id = owner.Name,
															Name = owner.Name,
															ThingName = owner.Thing.Name,
															SpecificProperty = owner.Thing.SpecificProperty
														});
			}
		}


		[Fact]
		public void CanGenerateIndex()
		{
			new SpecificThingOwners_AndTheirThings().CreateIndexDefinition();
		}
	}
}