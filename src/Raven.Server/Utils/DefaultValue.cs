namespace Raven.Server.Utils
{
    public class DefaultValue<T>
    {
        public static DefaultValue<T> Default = new DefaultValue<T>()
        {
            Value = default(T)
        }; 

        public T Value;
    }
}