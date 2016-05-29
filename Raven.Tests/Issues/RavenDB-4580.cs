using System.Collections.Generic;
using System.Linq;
using Raven.Tests.Common;
using Raven.Tests.Helpers;
using Xunit;


namespace Raven.Tests.Issues
{
    public class RavenDB_4580 : RavenTestBase
    {

        [Fact]
        public void TestWhatChangedObjectFieldNameCorrect()
        {
            var car = new Car(new List<Owner>
            {
                new Owner
                {
                    FirstName = "Fedia",
                    LastName = "Petkin"
                }
            })
            {

                Make = "Ford",
                CurrentOwner = new Owner { FirstName = "Ned" },
                PreviousOwner = new Owner { FirstName = "Adama" }

            };

            var carId = "Car/1";

            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(car, carId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var returnedCar = session.Load<Car>(carId);
                    returnedCar.CurrentOwner.FirstName = "Arya";
                    returnedCar.PreviousOwner.FirstName = "Starbuck";
                    returnedCar.Owners[0].FirstName = "joly";
                    returnedCar.Owners.Add(new Owner { FirstName = "et" });

                    var whatChanged = session.Advanced.WhatChanged().Where(changes => changes.Key == returnedCar.Id);
                    foreach (var value in whatChanged.SelectMany(change => change.Value.Where(value => value.FieldNewValue == "Starbuck")))
                    {
                        Assert.Equal(value.FieldName, "PreviousOwner.FirstName");
                    }
                }
            }
        }


        [Fact]
        public void TestWhatChangedArrayFieldNameCorrect()
        {
            var car = new Car(new List<Owner>())
            {
                Make = "Ford",
                CurrentOwner = new Owner { FirstName = "Ned" },
                PreviousOwner = new Owner { FirstName = "Adama" }
            };

            car.Owners.Add(new Owner { FirstName = "Sam" });
            var carId = "Car/1";

            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(car, carId);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var returnedCar = session.Load<Car>(carId);
                    returnedCar.CurrentOwner.FirstName = "Arya";
                    returnedCar.PreviousOwner.FirstName = "Starbuck";

                    returnedCar.Owners[0].FirstName = "Frodo";

                    var whatChanged = session.Advanced.WhatChanged().Where(changes => changes.Key == returnedCar.Id);

                    foreach (var value in whatChanged.SelectMany(change => change.Value.Where(value => value.FieldNewValue == "Arya")))
                    {
                        Assert.Equal(value.FieldName, "CurrentOwner.FirstName");
                    }

                    foreach (var value in whatChanged.SelectMany(change => change.Value.Where(value => value.FieldNewValue == "Starbuck")))
                    {
                        Assert.Equal(value.FieldName, "PreviousOwner.FirstName");

                    }

                    foreach (var value in whatChanged.SelectMany(change => change.Value.Where(value => value.FieldNewValue == "Frodo")))
                    {
                        Assert.Equal(value.FieldName, "Owners[0].FirstName");
                    }
                }
            }
        }

    }

    public class Car
    {
        public string Id { get; set; }
        public string Make { get; set; }
        public Owner CurrentOwner { get; set; }
        public Owner PreviousOwner { get; set; }
        public List<Owner> Owners { get; set; }

        public Car() { }

        public Car(List<Owner> owners)
        {
            Owners = owners;
        }
    }

    public class Owner
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
    }
}
