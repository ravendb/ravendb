using System;

namespace Indexing.Benchmark.Entities
{
    public class Employee
    {
        public string LastName { get; set; }
        public string FirstName { get; set; }
        public string Title { get; set; }
        public Address Address { get; set; }
        public DateTimeOffset HiredAt { get; set; }
        public DateTimeOffset Birthday { get; set; }
        public string HomePhone { get; set; }
        public int Extension { get; set; }
        public string ReportsTo { get; set; }
        public object Notes { get; set; }
        public int[] Territories { get; set; }
    }
}