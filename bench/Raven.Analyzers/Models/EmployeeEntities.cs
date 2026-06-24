namespace Raven.Analyzers.Playground.Models;

public class Employee
{
    public string Id { get; set; } = default!;
    public string FirstName { get; set; } = default!;
    public string LastName { get; set; } = default!;
    public string Title { get; set; } = default!;
    public string DepartmentId { get; set; } = default!;
    public string ManagerId { get; set; } = default!;
    public Address HomeAddress { get; set; } = default!;
    public decimal Salary { get; set; }
}

public class Address
{
    public string Street { get; set; } = default!;
    public string City { get; set; } = default!;
    public string PostalCode { get; set; } = default!;
    public string Country { get; set; } = default!;
}

public class Department
{
    public string Id { get; set; } = default!;
    public string Name { get; set; } = default!;
    public string ManagerId { get; set; } = default!;
    public string Budget { get; set; } = default!;
}

public class Manager
{
    public string Id { get; set; } = default!;
    public string Name { get; set; } = default!;
    public string DepartmentId { get; set; } = default!;
    public int DirectReports { get; set; }
}
