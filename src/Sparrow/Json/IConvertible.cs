namespace Sparrow.Json
{
    public interface IConvertible<out T>
        where T : class
    {
        T Convert();
    }
}
