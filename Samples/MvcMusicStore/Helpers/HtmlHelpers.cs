using System.Web.Mvc;

namespace MvcMusicStore
{
    public static class HtmlHelpers
    {
        /// <summary>
        /// This is a simple HTML Helper which truncates a string to a given length
        /// </summary>
        /// <param name="helper">HTML Helper being extended</param>
        /// <param name="input">Input string to truncate</param>
        /// <param name="length">Max length of the string</param>
        /// <returns></returns>
        public static string Truncate(this HtmlHelper helper, string input, int length)
        {
            if (input.Length <= length)
            {
                return input;
            }
            else
            {
                return input.Substring(0, length) + "...";
            }
        }
    }
}