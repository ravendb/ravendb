using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using FastTests.Server.NotificationCenter;
using Orders;
using Raven.Client.Documents;
using SlowTests.Smuggler;

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

                using (var a = new FastTests.Server.NotificationCenter.NotificationCenterTests())
                {
                    try
                    {
                        a.Can_dismiss_persistent_action_and_get_notified_about_it();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        Console.ReadLine();
                    }
                }
            }
        }
    }
}
