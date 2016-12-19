// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3042.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3042 : IDisposable
    {
        public void Dispose()
        {
        }

        [Fact]
        public void RavenJToken_DeepEquals_Array_Test()
        {
            var original = new {Tokens = new[] {"Token-1", "Token-2", "Token-3"}};
            var modified = new {Tokens = new[] {"Token-1", "Token-3"}};

            // In modified object we deleted one item "Token-2"

            var difference = new List<DocumentsChanges>();
            if (!RavenJToken.DeepEquals(RavenJObject.FromObject(modified), RavenJObject.FromObject(original), difference))
            {
                // OK
                // 1 difference - "Token-2" value removed
            }

            // Expecting two difference - "Token-3" ArrayValueRemoved,"Token-2" changed
            Assert.True(difference.Count == 2 && difference.SingleOrDefault(x => x.Change == DocumentsChanges.ChangeType.FieldChanged &&
                                                                                 x.FieldOldValue == "Token-2") != null);

            var originalDoc = new Doc {Names = new List<PersonName> {new PersonName {Name = "Tom1"}, new PersonName {Name = "Tom2"}, new PersonName {Name = "Tom3"}}};
            var modifiedDoc = new Doc {Names = new List<PersonName> {new PersonName {Name = "Tom1"}, new PersonName {Name = "Tom3"}}};

            // In modified object we deleted one item "Tom2"

            difference = new List<DocumentsChanges>();
            if (!RavenJToken.DeepEquals(RavenJObject.FromObject(modifiedDoc), RavenJObject.FromObject(originalDoc), difference))
            {
                // SOMETHING WRONG?
                // 3 differences - "Tom1", "Tom2", "Tom3" objects removed
            }

            // Expecting two difference - "Tom3" ArrayValueRemoved,Tom2 field change
            Assert.True(difference.Count == 2 && difference.Where(x => x.Change == DocumentsChanges.ChangeType.ArrayValueRemoved &&
                                                                                 x.FieldOldValue == "{\r\n  \"Name\": \"Tom3\"\r\n}") != null);
        }


        public class Doc
        {
            public List<PersonName> Names { get; set; }
        }

        public class PersonName
        {
            public string Name { get; set; }
        }
    }
}
