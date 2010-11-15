namespace Raven.Tests
{
    public class Some
    {
        static int index = 0;

        static public int Integer()
        {
            return index++;
        }

        static public string String()
        {
            return "someString" + Integer().ToString();
        }
    }
}
