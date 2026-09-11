using System.Linq;
using Raven.Analyzers.Playground.Models;
using Raven.Client.Documents.Indexes;

namespace Raven.Analyzers.Playground.Indexes;

public class Employees_ByDepartment : AbstractIndexCreationTask<Employee>
{
    public Employees_ByDepartment()
    {
        Map = employees => from e in employees
                           select new
                           {
                               e.DepartmentId,
                               e.Title,
                               e.ManagerId,
                               FullName = e.FirstName + " " + e.LastName
                           };
    }
}

public class Employees_Search : AbstractIndexCreationTask<Employee>
{
    public Employees_Search()
    {
        Map = employees => from e in employees
                           select new
                           {
                               e.FirstName,
                               e.LastName,
                               e.Title,
                               e.DepartmentId
                           };

        Index(x => x.FirstName, FieldIndexing.Search);
        Index(x => x.LastName, FieldIndexing.Search);
    }
}
