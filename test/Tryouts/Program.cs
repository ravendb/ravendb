using System;
using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var h = new HttpClient();
            h.GetAsync("http://google.com").Wait();
        }
    }
}