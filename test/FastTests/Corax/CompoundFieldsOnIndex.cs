using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class CompoundFieldsOnIndex : RavenTestBase
{
    public CompoundFieldsOnIndex(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void CanDefineIndexWithCompoundFieldAndReadItBack()
    {
        using var store = GetDocumentStore();
        var index = new Users_ByNameAndBirthday();
        index.Execute(store);
        var definition = store.Maintenance.Send(new GetIndexOperation(index.IndexName));
        Assert.NotEmpty(definition.CompoundFields);
    }


    private record User(string Name, DateTime Birthday);

    private class Users_ByNameAndBirthday : AbstractIndexCreationTask<User>
    {
        public Users_ByNameAndBirthday()
        {
            Map = users => 
                from u in users 
                select new { u.Name, u.Birthday };

            CompoundFields.Add(new[] { nameof(User.Name), nameof(User.Birthday) });
        }
    }
}
