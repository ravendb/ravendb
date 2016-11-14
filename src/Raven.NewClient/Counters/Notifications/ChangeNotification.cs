namespace Raven.NewClient.Abstractions.Counters.Notifications
{
    public class ChangeNotification : CounterStorageNotification
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

    public class StartingWithNotification : ChangeNotification
    {
        public override string ToString()
        {
            var sign = Delta >= 0 ? "+" : "";
            return $"StartingWithNotification({GroupName}:{CounterName}{sign}{Delta},Total={Total})";
        }
    }

    public class InGroupNotification : ChangeNotification
    {
        public override string ToString()
        {
            var sign = Delta >= 0 ? "+" : "";
            return $"InGroupNotification({GroupName}:{CounterName}{sign}{Delta},Total={Total})";
        }
    }
}
