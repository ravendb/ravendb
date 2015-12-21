// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4144.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4144 : RavenTest
    {
        [Fact]
        public void can_save_javascript_array_values()
        {
            using (var store = NewDocumentStore())
            {
                var foo = new Foo
                {
                    List = {"test"}
                };

                using (var session = store.OpenSession())
                {
                    session.Store(foo);
                    session.SaveChanges();
                }

                store.DatabaseCommands.Patch(foo.Id, new ScriptedPatchRequest
                {
                    Script = @"var list = ['my', 'list']; 
                                for(var x in list){
                                    this.List[x] = list[x];
                                }"
                });
            }
        }

        public class Foo
        {
            public Foo()
            {
                List = new List<string>();    
            }

            public string Id { get; set; }

            public List<string> List { get; set; }
        }
    }
}
