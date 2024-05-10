namespace Voron
{
    public class ReadResult(ValueReader reader)
    {
        public ValueReader Reader { get; private set; } = reader;
    }
}
