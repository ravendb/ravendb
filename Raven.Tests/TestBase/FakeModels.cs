namespace Raven.Tests.TestBase
{
    public abstract class AggregateRoot
    {
        public string Id { get; set; }
    }

    public abstract class Animal
    {
        public string Name { get; set; }
    }

    public class Cat : Animal
    {
        public int PurringCount { get; set; }
    }

    public class Dog : Animal
    {
        public int BarkCount { get; set; }
    }

    public class Home
    {
        public string Address { get; set; }
    }
}