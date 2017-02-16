namespace Sparrow.Json
{
    public interface IConvertible<out T>
        where T : class
    {
        bool CanConvert();

        T Convert();
    }
}
