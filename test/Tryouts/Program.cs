using System;
using System.IO;
using Raven.Server.Utils;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var c = DiskSpaceChecker.GetFreeDiskSpace(@"/home/lili/Sources/ravendb", DriveInfo.GetDrives());
            Console.WriteLine("Name:  " + c.DriveName);
            Console.WriteLine("Free:  " + c.TotalFreeSpace);
            Console.WriteLine("Total: " + c.TotalSize);
        }
    }
}
