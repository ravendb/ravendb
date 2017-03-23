using System;
using System.Diagnostics;
using FastTests.Client.Attachments;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            Console.WriteLine();

            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var a = new AttachmentsReplication())
                {
                    a.PutAndDeleteAttachmentsWithTheSameStream_AlsoTestBigStreams();
                }
            }
        }
    }
}