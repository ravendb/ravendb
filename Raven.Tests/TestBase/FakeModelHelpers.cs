using System;
using System.Collections.Generic;

namespace Raven.Tests.TestBase
{
    public static class FakeModelHelpers
    {
        public static IEnumerable<Cat> CreateFakeCats(int numberOfCats = 20)
        {
            if (numberOfCats <= 0)
            {
                throw new ArgumentOutOfRangeException("numberOfCats");
            }

            var result = new List<Cat>();
            for (var i = 1; i <= numberOfCats; i++)
            {
                result.Add(new Cat
                {
                    Name = "Some Cat " + i,
                    PurringCount = i
                });
            }

            return result;
        }

        public static IEnumerable<Dog> CreateFakeDogs(int numberOfDogs = 20)
        {
            if (numberOfDogs <= 0)
            {
                throw new ArgumentOutOfRangeException("numberOfDogs");
            }

            var result = new List<Dog>();
            for (var i = 1; i <= numberOfDogs; i++)
            {
                result.Add(new Dog
                {
                    Name = "Some Dog " + i,
                    BarkCount = i
                });
            }

            return result;
        }
    }
}