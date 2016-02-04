// -----------------------------------------------------------------------
//  <copyright file="RavenDB_25xx.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;

using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2566 : RavenTest
    {
        [Fact]
        public void ChangesApiForDocumentsOfTypeShoudUseConventionsToGetTypeName()
        {
            using (var store = NewDocumentStore())
            {
                store.Conventions.FindClrTypeName = type => "custom";

                var mre = new ManualResetEventSlim();

                store
                    .Changes().Task.Result
                    .ForDocumentsOfType<Person>().Task.Result
                    .Subscribe(change => mre.Set());

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Name = "Name1" });
                    session.SaveChanges();
                }

                Assert.True(mre.Wait(5000));
            }
        }
    }
}
