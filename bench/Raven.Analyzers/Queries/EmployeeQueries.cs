using System.Collections.Generic;
using System.Linq;
using Raven.Analyzers.Playground.Models;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;

namespace Raven.Analyzers.Playground.Queries;

public static class EmployeeQueries
{
    public static List<Employee> GetByDepartment(IDocumentSession session, string departmentId)
        => session.Query<Employee>()
                  .Where(e => e.DepartmentId == departmentId)
                  .OrderBy(e => e.LastName)
                  .Take(100)
                  .ToList();

    public static List<Employee> GetByManager(IDocumentSession session, string managerId)
        => session.Query<Employee>()
                  .Where(e => e.ManagerId == managerId)
                  .Take(50)
                  .ToList();

    public static List<EmployeeView> GetViews(IDocumentSession session, string title)
        => session.Query<Employee>()
                  .Where(e => e.Title == title)
                  .OrderBy(e => e.FirstName)
                  .Take(30)
                  .Select(e => new EmployeeView { Id = e.Id, FullName = e.FirstName + " " + e.LastName })
                  .ToList();

    public static Employee? FindById(IDocumentSession session, string id)
        => session.Query<Employee>()
                  .Where(e => e.Id == id)
                  .Take(1)
                  .FirstOrDefault();
}

public class EmployeeView
{
    public string Id { get; set; } = default!;
    public string FullName { get; set; } = default!;
}
