// -----------------------------------------------------------------------
//  <copyright file="AmbiguousMatchExceptionTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class AmbiguousMatchExceptionTest : NoDisposalNeeded
    {
        private class GeneralThing
        {
            public string Name { get; set; }
        }

        private class SpecificThing : GeneralThing
        {
            public string SpecificProperty { get; set; }
        }

        private class GeneralThingOwner
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public GeneralThing Thing { get; set; }
        }

        private class SpecificThingOwner : GeneralThingOwner
        {
            public new SpecificThing Thing { get; set; }
        }

        private class SpecificThingOwners_AndTheirThings : AbstractIndexCreationTask<SpecificThingOwner>
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
            }
        }

        private class SpecificThingOwners_AndTheirThingsTransformer : AbstractTransformerCreationTask<SpecificThingOwner>
        {
            public SpecificThingOwners_AndTheirThingsTransformer()
            {
                TransformResults = docs => docs
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
            new SpecificThingOwners_AndTheirThingsTransformer().CreateTransformerDefinition();
        }
    }
}
