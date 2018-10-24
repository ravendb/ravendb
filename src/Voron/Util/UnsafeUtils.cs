namespace Voron.Util
{
    public static unsafe class UnsafeUtils
    {
        //TODO : play around with it to make sure it works as I suspect it is
        public static byte* AddressOf<T>(this T val) where T : struct
        {
            var reference = __makeref(val);
            return (byte*)&reference;
        }
    }
}
