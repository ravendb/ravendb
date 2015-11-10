using System;
using NLog;
using NLog.Config;
using NLog.Targets;
using Rachis.Tests;

namespace Raven.Tryouts
{
    public class Program
    {
        public static void Main()
        {
            
            for (int i = 0; i < 10; i++)
            {
                Console.Clear();
                Console.WriteLine(i);
                using (var x = new TopologyChangesTests())
                {
                    x.Leader_removed_from_cluster_modifies_member_lists_on_remaining_nodes(2);
                }
            }
        }
    }
}
