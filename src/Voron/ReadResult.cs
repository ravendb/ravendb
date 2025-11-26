namespace Voron
{
    public class ReadResult
    {
        public ReadResult(ValueReader reader)
        {
            Reader = reader;
        }
         
        public ValueReader Reader { get; private set; }
    }
}
