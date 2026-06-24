using System;

namespace Raven.Analyzers.Playground.Models;

public class Lead
{
    public string Id { get; set; } = default!;
    public string Name { get; set; } = default!;
    public string Email { get; set; } = default!;
    public string OwnerId { get; set; } = default!;
    public string Status { get; set; } = default!;
    public decimal EstimatedValue { get; set; }
    public DateTime CreatedAt { get; set; }
}

public class Contact
{
    public string Id { get; set; } = default!;
    public string FirstName { get; set; } = default!;
    public string LastName { get; set; } = default!;
    public string AccountId { get; set; } = default!;
    public string Email { get; set; } = default!;
    public string Phone { get; set; } = default!;
}

public class Company
{
    public string Id { get; set; } = default!;
    public string Name { get; set; } = default!;
    public string Industry { get; set; } = default!;
    public int EmployeeCount { get; set; }
    public string OwnerId { get; set; } = default!;
}

public class Tag
{
    public string Id { get; set; } = default!;
    public string Name { get; set; } = default!;
    public string Color { get; set; } = default!;
}
