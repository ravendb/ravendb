using System;
using System.IO;

namespace Rhino.DivanDB.Client.Tests
{
    public class BaseTest
    {
        protected const string DbDirectory = @".\TestDb\";
        protected const string DbName = DbDirectory + @"DocDb.esb";

        public BaseTest()
        {
            if (Directory.Exists(DbDirectory))
                Directory.Delete(DbDirectory, true);

            Directory.CreateDirectory(DbDirectory);
        }

        public double Timer(Action action)
        {
            var startTime = DateTime.Now;
            action.Invoke();
            var timeTaken = DateTime.Now.Subtract(startTime);
            Console.WriteLine("Time take (ms)- " + timeTaken.TotalMilliseconds);
            return timeTaken.TotalMilliseconds;
        }
    }
}