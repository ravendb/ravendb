// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4010.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Database.Indexing;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4010 : RavenTest
    {
        [Fact]
        public void DefaultBackgroundExecuterMustNotProduceMoreParallelTasksThanAllowedNumberOfParallelTasks()
        {
            using (var store = NewDocumentStore())
            {
                var taskCount = 0;

                BackgroundTaskExecuter.Instance.ExecuteAllBuffered(store.DocumentDatabase.WorkContext, new byte[43690].ToList(), partition =>
                {
                    taskCount++;
                });

                var maxAllowed = store.DocumentDatabase.WorkContext.CurrentNumberOfParallelTasks;
                Assert.True(taskCount <= maxAllowed, string.Format("Max {0}, but was {1}", maxAllowed, taskCount));
            } 
        }
    }
}