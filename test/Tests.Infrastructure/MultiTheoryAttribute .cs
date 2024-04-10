using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Tests.Infrastructure
{
    public class MultiTheoryAttribute : TheoryAttribute
    {
        public MultiTheoryAttribute(params Type[] types)
        {
            var result = new List<string>();
            foreach (var t in types)
            {
                var theoryAttribute = Activator.CreateInstance(t);
                if (theoryAttribute is TheoryAttribute theory)
                {
                    if (string.IsNullOrEmpty(theory.Skip) == false)
                    {
                        result.Add(theory.Skip);
                    }
                } else if (theoryAttribute is FactAttribute fact)
                {
                    if (string.IsNullOrEmpty(fact.Skip) == false)
                    {
                        result.Add(fact.Skip);
                    }
                }
            }

            if (result.Any())
            {
                Skip = string.Join(", ", result);
            }
        }
    }
}
