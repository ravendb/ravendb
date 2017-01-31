namespace Raven.Abstractions.Counters.Notifications
{
    public class CounterChange : CounterStorageChange
    {
        public string GroupName { get; set; }

        public string CounterName { get; set; }

        public CounterChangeAction Action { get; set; }

        public long Delta { get; set; }

        public long Total { get; set; }
    }

    public enum CounterChangeAction
    {
        Add,
        Increment,
        Decrement,
        Delete
    }

    public class StartingWithChange : CounterChange
    {
        public override string ToString()
        {
            var sign = Delta >= 0 ? "+" : "";
            return $"StartingWithChange({GroupName}:{CounterName}{sign}{Delta},Total={Total})";
        }
    }

    public class InGroupChange : CounterChange
    {
        public override string ToString()
        {
            var sign = Delta >= 0 ? "+" : "";
            return $"InGroupChange({GroupName}:{CounterName}{sign}{Delta},Total={Total})";
        }
    }
}
