using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Client
{
    public class ForceRevisionCreation : RavenTestBase
    {
        [Fact]
        public async Task ForceRevisionCreationForSingleUnTrackedEntityByID()
        {
            using (var store = GetDocumentStore())
            {
                string companyId;
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company);
                    companyId = company.Id;
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    session.Advanced.Revisions.ForceRevisionCreationFor(companyId);
                    session.SaveChanges();

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(companyId).Count;
                    Assert.Equal(1, revisionsCount);
                }
            }
        }
         
        [Fact]
        public async Task ForceRevisionCreationForMultipleUnTrackedEntitiesByID()
        {
            using (var store = GetDocumentStore())
            {
                string companyId1;
                string companyId2;
                
                using (var session = store.OpenSession())
                {
                    var company1 = new Company { Name = "HR1" };
                    var company2 = new Company { Name = "HR2" };
                    session.Store(company1);
                    session.Store(company2);
                    
                    companyId1 = company1.Id;
                    companyId2 = company2.Id;
                    
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    session.Advanced.Revisions.ForceRevisionCreationFor(companyId1);
                    session.Advanced.Revisions.ForceRevisionCreationFor(companyId2);
                    session.SaveChanges();
                    
                    var revisionsCount1 = session.Advanced.Revisions.GetFor<Company>(companyId1).Count; 
                    var revisionsCount2 = session.Advanced.Revisions.GetFor<Company>(companyId2).Count;
                    
                    Assert.Equal(1, revisionsCount1);
                    Assert.Equal(1, revisionsCount2);
                }
            }
        }
        
        [Fact]
        public async Task CannotForceRevisionCreationForUnTrackedEntityByEntity()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                  
                    var ex = Assert.Throws<InvalidOperationException>(() => session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company));
                    Assert.Contains("Cannot create a revision for the requested entity because it is not tracked by the session", ex.Message);
                }
            }
        }
        
        [Fact]
        public async Task ForceRevisionCreationForNewDocumentByEntity()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company);
                    session.SaveChanges();
                    
                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company); 
                   
                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count; 
                    Assert.Equal(0, revisionsCount); 
                    
                    session.SaveChanges();
                    
                    revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count; 
                    Assert.Equal(1, revisionsCount); 
                }
            }
        }
        
        [Fact]
        public async Task CannotForceRevisionCreationForNewDocumentBeforeSavingToServerByEntity()
        {
             using (var store = GetDocumentStore())
             {
                 using (var session = store.OpenSession())
                 {
                     var company = new Company { Name = "HR" };
                     session.Store(company);
                     
                     session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);
                   
                     var ex = Assert.Throws<RavenException>(() => session.SaveChanges());
                     Assert.Contains("Can't force revision creation - the document is not saved on the server yet.", ex.Message);
    
                     var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                     Assert.Equal(0, revisionsCount);
                 }
             }
        }
        
        [Fact]
        public async Task ForceRevisionCreationForTrackedEntityWithNoChangesByEntity()
        {
            using (var store = GetDocumentStore())
            {
                var companyId = "";
                
                using (var session = store.OpenSession())
                {
                    // 1. Store document
                    var company = new Company { Name = "HR" };
                    session.Store(company);
                    session.SaveChanges();
                    companyId = company.Id;

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(0, revisionsCount);
                }

                using (var session = store.OpenSession())
                {
                    // 2. Load & Save without making changes to the document 
                    var company = session.Load<Company>(companyId);
                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);
                    session.SaveChanges();

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(companyId).Count;
                    Assert.Equal(1, revisionsCount);
                }
            }
        }
        
        [Fact]
        public async Task ForceRevisionCreationForTrackedEntityWithChangesByEntity()
        {
            using (var store = GetDocumentStore())
            {
                var companyId = "";
                
                // 1. Store document
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company);
                    session.SaveChanges();
                    companyId = company.Id;

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(0, revisionsCount);
                }
                
                using (var session = store.OpenSession())
                {
                    // 2. Load, Make changes & Save 
                    var company = session.Load<Company>(companyId);
                    company.Name = "HR V2";
                    session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);
                    session.SaveChanges();
                    
                    List<Company> revisions = session.Advanced.Revisions.GetFor<Company>(company.Id);
                    var revisionsCount = revisions.Count;
                    
                    Assert.Equal(1, revisionsCount);
                    Assert.Equal("HR", revisions[0].Name); 
                }
            }
        }
        
        [Fact]
        public async Task ForceRevisionCreationForTrackedEntityWithChangesByID()
        {
            using (var store = GetDocumentStore())
            {
                var companyId = "";
                
                // 1. Store document
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company);
                    session.SaveChanges();
                    companyId = company.Id;

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(0, revisionsCount);
                }
                
                using (var session = store.OpenSession())
                {
                    // 2. Load, Make changes & Save 
                    var company = session.Load<Company>(companyId); 
                    company.Name = "HR V2";
                    session.Advanced.Revisions.ForceRevisionCreationFor(company.Id);
                    
                    session.SaveChanges();

                    List<Company> revisions = session.Advanced.Revisions.GetFor<Company>(company.Id);
                    var revisionsCount = revisions.Count;
                    
                    Assert.Equal(1, revisionsCount);
                    Assert.Equal("HR", revisions[0].Name); 
                }
            }
        }
        
        [Fact]
        public async Task ForceRevisionCreationMultipleRequests()
        {
            using (var store = GetDocumentStore())
            {
                var companyId = "";
               
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company);
                    session.SaveChanges();
                    companyId = company.Id;

                    var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                    Assert.Equal(0, revisionsCount);
                }
                
                using (var session = store.OpenSession())
                {
                    session.Advanced.Revisions.ForceRevisionCreationFor(companyId);
                    
                    var company = session.Load<Company>(companyId); 
                    company.Name = "HR V2";

                    var ex = Assert.Throws<InvalidOperationException>(() => session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company));
                    Assert.Contains("A request for creating a revision was already made", ex.Message);
                    
                    ex = Assert.Throws<InvalidOperationException>(() => session.Advanced.Revisions.ForceRevisionCreationFor(company.Id));
                    Assert.Contains("A request for creating a revision was already made", ex.Message);
                    
                    session.SaveChanges();

                    List<Company> revisions = session.Advanced.Revisions.GetFor<Company>(company.Id);
                    var revisionsCount = revisions.Count;
                    
                    Assert.Equal(1, revisionsCount);
                    Assert.Equal("HR", revisions[0].Name); 
                }
            }
        }
    }
}
