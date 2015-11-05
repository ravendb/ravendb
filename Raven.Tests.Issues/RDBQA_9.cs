// -----------------------------------------------------------------------
//  <copyright file="RDBQA_9.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

using Raven.Abstractions.Database.Smuggler.Common;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RDBQA_9 : RavenTest
    {
        [Fact]
        public void SmugglerShouldSupportFiltersWithCommas()
        {
            Assert.Equal(new List<string> { "1", "2", "3" }, FilterSetting.ParseValues("1,2,3"));
            Assert.Equal(new List<string> { "1", "2,3" }, FilterSetting.ParseValues("1,'2,3'"));
            Assert.Equal(new List<string> { "1,2", "3" }, FilterSetting.ParseValues("'1,2',3"));
            Assert.Equal(new List<string> { "1,2,3" }, FilterSetting.ParseValues("'1,2,3'"));
            Assert.Equal(new List<string>(), FilterSetting.ParseValues(null));
            Assert.Equal(new List<string>(), FilterSetting.ParseValues(string.Empty));
        }
    }
}
