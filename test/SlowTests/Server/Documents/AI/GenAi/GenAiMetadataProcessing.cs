using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.GenAi
{
    public class GenAiMetadataProcessing(ITestOutputHelper output) : RavenTestBase(output)
    {
        private class CustomerSupport
        {
            public string Id { get; set; }
            public string TicketId { get; set; }
            public string Subject { get; set; }
            public string Description { get; set; }
            public List<Message> Messages { get; set; }
            public string Status { get; set; }
            public string AICategory { get; set; }
            public int UrgencyScore { get; set; }
            public List<string> Tags { get; set; }
        }

        private class Message
        {
            public string Id { get; set; }
            public string Author { get; set; }
            public string Content { get; set; }
            public DateTime Timestamp { get; set; }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanAccessAndUpdateMetadataInGenAiProcessing(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var etl = Etl.WaitForEtlToComplete(store);

            config.Prompt = "Analyze the support ticket and determine priority, category, and routing based on content and customer metadata";
            config.Collection = "CustomerSupports";
            config.SampleObject = JsonConvert.SerializeObject(new 
            { 
                Priority = "High/Medium/Low",
                Category = "Technical/Billing/General",
                RequiresEscalation = true,
                Urgency = 1, // 1-5 scale
                Tags = new[] { "tag1", "tag2" },
                SuggestedTeam = "Engineering/Support/Sales",
                Reason = "Brief explanation"
            });
            
            // Comprehensive transformation script that accesses all types of metadata
            config.GenAiTransformation = new GenAiTransformation
            {
                Script = @"
// Access document metadata
const metadata = this['@metadata'];

// Skip processing based on metadata conditions
if (metadata.processed === true || metadata.trustedUser === true) {
    return;
}

// Build comprehensive context from document and metadata
const context = {
    // Document fields
    TicketId: this.TicketId,
    Subject: this.Subject,
    Description: this.Description,
    Status: this.Status,
    MessageCount: this.Messages.length,
    
    // Customer metadata (simple fields)
    CustomerTier: metadata.customerTier || 'Standard',
    AccountAge: metadata.accountAge || 0,
    PreviousTickets: metadata.previousTickets || 0,
    ContractValue: metadata.contractValue || 0,
    
    // Nested metadata structures
    SupportHistory: metadata.supportHistory || {
        avgResolutionHours: 24,
        satisfactionScore: 3
    },
    
    // Computed values from metadata
    IsVIP: metadata.customerTier === 'Enterprise' || metadata.contractValue > 100000,
    
    // Messages with metadata context
    Messages: this.Messages.map(m => ({
        Author: m.Author,
        Content: m.Content,
        UserReputation: metadata.userReputation || 0
    }))
};

ai.genContext(context);
"
            };
            
            config.UpdateScript = @"
// Update document fields
this.Status = $output.RequiresEscalation ? 'Escalated' : this.Status;
this.AICategory = $output.Category;
this.UrgencyScore = $output.Urgency;

if (!this.Tags) {
    this.Tags = [];
}
this.Tags = this.Tags.concat($output.Tags);

// Update metadata with AI analysis
const metadata = this['@metadata'];
metadata.aiPriority = $output.Priority;
metadata.aiCategory = $output.Category;
metadata.suggestedTeam = $output.SuggestedTeam;
metadata.aiReason = $output.Reason;
metadata.processed = true;
metadata.processedAt = new Date().toISOString();

// Track categorization history in nested structure
if (!metadata.aiHistory) {
    metadata.aiHistory = [];
}
metadata.aiHistory.push({
    category: $output.Category,
    priority: $output.Priority,
    analyzedAt: new Date().toISOString(),
    urgency: $output.Urgency
});

// Set escalation flag if high urgency
if ($output.Urgency >= 4) {
    metadata.escalationRequired = true;
}
";

            store.Maintenance.Send(new AddGenAiOperation(config));

            using (var session = store.OpenSession())
            {
                // Create a VIP ticket with rich metadata
                var vipTicket = new CustomerSupport
                {
                    TicketId = "SUP-12345",
                    Subject = "Critical system outage affecting production",
                    Description = "Our production environment is completely down and we're losing revenue",
                    Messages = new List<Message>
                    {
                        new Message
                        {
                            Id = "msg1",
                            Author = "cto@enterprise.com",
                            Content = "This is urgent! Our entire system is down!",
                            Timestamp = DateTime.UtcNow.AddMinutes(-30)
                        }
                    },
                    Status = "Open"
                };
                
                session.Store(vipTicket);
                
                // Add comprehensive metadata
                var metadata = session.Advanced.GetMetadataFor(vipTicket);
                metadata["customerTier"] = "Enterprise";
                metadata["accountAge"] = 5;
                metadata["previousTickets"] = 3;
                metadata["contractValue"] = 500000;
                metadata["userReputation"] = 100;
                metadata["supportHistory"] = JObject.FromObject(new
                {
                    avgResolutionHours = 4,
                    satisfactionScore = 4.8
                });
                
                // Create a regular ticket that should be skipped
                var trustedTicket = new CustomerSupport
                {
                    TicketId = "TRUST-001",
                    Subject = "Regular question",
                    Messages = new List<Message>
                    {
                        new Message
                        {
                            Id = "msg1",
                            Author = "trusted@customer.com",
                            Content = "Some regular content",
                            Timestamp = DateTime.UtcNow
                        }
                    },
                    Status = "Open"
                };
                session.Store(trustedTicket);
                var trustedMetadata = session.Advanced.GetMetadataFor(trustedTicket);
                trustedMetadata["trustedUser"] = true;
                
                session.SaveChanges();
            }

            Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(30)));

            // Verify metadata was processed correctly
            using (var session = store.OpenSession())
            {
                var vipTicket = session.Query<CustomerSupport>()
                    .First(t => t.TicketId == "SUP-12345");
                var metadata = session.Advanced.GetMetadataFor(vipTicket);
                
                // Verify AI processed the metadata
                Assert.True(metadata.ContainsKey("aiPriority"));
                Assert.True(metadata.ContainsKey("aiCategory"));
                Assert.True(metadata.ContainsKey("suggestedTeam"));
                Assert.True(metadata.ContainsKey("processed"));
                Assert.Equal(true, metadata["processed"]);
                
                // Verify history tracking
                Assert.True(metadata.ContainsKey("aiHistory"));
                var history = metadata["aiHistory"] as JArray;
                Assert.NotNull(history);
                Assert.NotEmpty(history);
                
                // Verify document was updated
                Assert.NotNull(vipTicket.AICategory);
                Assert.True(vipTicket.UrgencyScore > 0);
                
                // Verify trusted user ticket was not processed
                var trustedTicket = session.Query<CustomerSupport>()
                    .First(t => t.TicketId == "TRUST-001");
                var trustedMeta = session.Advanced.GetMetadataFor(trustedTicket);
                Assert.False(trustedMeta.ContainsKey("processed"));
            }
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanWorkWithNestedMetadataStructures(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var etl = Etl.WaitForEtlToComplete(store);

            config.Prompt = "Analyze content for SEO optimization and content improvement";
            config.Collection = "ContentItems";
            config.SampleObject = JsonConvert.SerializeObject(new 
            { 
                SeoTitle = "Optimized title",
                MetaDescription = "Description for search engines",
                Keywords = new[] { "keyword1", "keyword2" },
                ReadabilityScore = 8.5,
                ContentSuggestions = new[] { "suggestion1", "suggestion2" },
                TargetAudienceMatch = 0.9
            });
            
            config.GenAiTransformation = new GenAiTransformation
            {
                Script = @"
const metadata = this['@metadata'];

// Access nested metadata structures
const seo = metadata.seo || {};
const analytics = metadata.analytics || {};
const audience = metadata.audience || {};

// Build context from nested metadata
const context = {
    Content: {
        Title: this.Title,
        Body: this.Content,
        Author: this.Author
    },
    
    CurrentSEO: {
        Title: seo.title,
        Description: seo.description,
        Keywords: seo.keywords || []
    },
    
    Analytics: {
        Views: analytics.pageViews || 0,
        EngagementTime: analytics.avgTimeSeconds || 0,
        BounceRate: analytics.bounceRate || 0
    },
    
    TargetAudience: {
        Primary: audience.primary || 'General',
        Interests: audience.interests || [],
        Demographics: audience.demographics || {}
    }
};

ai.genContext(context);
"
            };
            
            config.UpdateScript = @"
const metadata = this['@metadata'];

// Update nested SEO metadata
if (!metadata.seo) {
    metadata.seo = {};
}
metadata.seo.title = $output.SeoTitle;
metadata.seo.description = $output.MetaDescription;
metadata.seo.keywords = $output.Keywords;
metadata.seo.lastOptimized = new Date().toISOString();
metadata.seo.optimizationScore = $output.ReadabilityScore;

// Update AI recommendations in nested structure
if (!metadata.ai) {
    metadata.ai = {};
}
metadata.ai.recommendations = {
    readabilityScore: $output.ReadabilityScore,
    suggestions: $output.ContentSuggestions,
    targetAudienceMatch: $output.TargetAudienceMatch,
    generatedAt: new Date().toISOString()
};

// Maintain optimization history
if (!metadata.optimizationHistory) {
    metadata.optimizationHistory = [];
}
metadata.optimizationHistory.push({
    timestamp: new Date().toISOString(),
    score: $output.ReadabilityScore,
    keywordCount: $output.Keywords.length
});

// Only keep last 5 history entries
if (metadata.optimizationHistory.length > 5) {
    metadata.optimizationHistory = metadata.optimizationHistory.slice(-5);
}
";

            store.Maintenance.Send(new AddGenAiOperation(config));

            using (var session = store.OpenSession())
            {
                var content = new ContentItem
                {
                    Title = "Getting Started with RavenDB",
                    Content = "RavenDB is a NoSQL document database that makes it easy to build scalable applications...",
                    Author = "Tech Writer",
                    CreatedAt = DateTime.UtcNow
                };
                
                session.Store(content);
                
                var metadata = session.Advanced.GetMetadataFor(content);
                
                // Add complex nested metadata
                metadata["seo"] = JObject.FromObject(new
                {
                    title = "RavenDB Tutorial",
                    description = "Learn RavenDB basics",
                    keywords = new[] { "database", "nosql" }
                });
                
                metadata["analytics"] = JObject.FromObject(new
                {
                    pageViews = 1500,
                    avgTimeSeconds = 240,
                    bounceRate = 0.25
                });
                
                metadata["audience"] = JObject.FromObject(new
                {
                    primary = "Developers",
                    demographics = new
                    {
                        experienceLevel = "Intermediate",
                        primaryLanguage = "C#"
                    },
                    interests = new[] { "databases", "performance", "scalability" }
                });
                
                session.SaveChanges();
            }

            Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(30)));

            // Verify nested metadata was processed
            using (var session = store.OpenSession())
            {
                var content = session.Query<ContentItem>().First();
                var metadata = session.Advanced.GetMetadataFor(content);
                
                // Check nested AI structure was created
                Assert.True(metadata.ContainsKey("ai"));
                var ai = metadata["ai"] as JObject;
                Assert.NotNull(ai);
                Assert.True(ai.ContainsKey("recommendations"));
                
                // Check SEO was updated
                var seo = metadata["seo"] as JObject;
                Assert.NotNull(seo);
                Assert.True(seo.ContainsKey("lastOptimized"));
                Assert.True(seo.ContainsKey("optimizationScore"));
                
                // Check history exists
                Assert.True(metadata.ContainsKey("optimizationHistory"));
                var history = metadata["optimizationHistory"] as JArray;
                Assert.NotNull(history);
                Assert.NotEmpty(history);
            }
        }

        private class ContentItem
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public string Content { get; set; }
            public string Author { get; set; }
            public DateTime CreatedAt { get; set; }
        }
    }
}
