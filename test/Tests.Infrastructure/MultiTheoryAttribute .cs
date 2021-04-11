using System;
using System.Linq;
using Xunit;

namespace Tests.Infrastructure
{
    public class MultiTheoryAttribute : TheoryAttribute
    {
        public MultiTheoryAttribute(params Type[] types)
        {
            var result = types.Select(Activator.CreateInstance).Cast<TheoryAttribute>().ToList();

            if (result.Any(x => string.IsNullOrEmpty(x.Skip) == false))
            {
                Skip = string.Join(", ", result.Where(x => string.IsNullOrEmpty(x.Skip) == false).Select(x => x.Skip));
            }
        }
    }
}
