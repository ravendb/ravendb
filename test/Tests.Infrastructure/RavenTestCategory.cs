﻿using System;
using System.ComponentModel;

namespace Tests.Infrastructure;

/// <summary>
/// Test categories for RavenDB tests. These categories are used to organize and filter tests based on functionality.
/// Categories can be combined using the | operator for tests that span multiple areas.
///
/// USAGE GUIDELINES:
/// - Use [RavenFact(RavenTestCategory.CategoryName)] for single test methods
/// - Use [RavenTheory(RavenTestCategory.CategoryName)] for parameterized tests
/// - Combine categories with | for tests covering multiple areas: RavenTestCategory.Querying | RavenTestCategory.Indexes
/// - Some categories have special requirements (licenses, environment variables, platform restrictions)
/// - Tests are automatically skipped if requirements are not met
///
/// SPECIAL REQUIREMENTS:
/// - LicenseRequired = true: Requires RAVEN_LICENSE environment variable
/// - NightlyBuildRequired = true: Only runs in nightly builds
/// - S3Required = true: Requires S3 configuration
/// - AzureRequired = true: Requires Azure configuration
/// - Platform restrictions: Use RavenMultiplatformFact/Theory for platform-specific tests
/// </summary>
[Flags]
public enum RavenTestCategory : long
{
    /// <summary>
    /// Default value - no specific category assigned.
    /// USAGE: Avoid using None, always specify appropriate category.
    /// </summary>
    None = 0,

    // ===== CORE & INFRASTRUCTURE =====

    /// <summary>
    /// Tests for core RavenDB functionality and infrastructure that do not belong to anything more specific.
    /// USAGE: Tests involving fundamental operations, core data structures, basic infrastructure.
    /// COMBINE WITH: Memory, Voron, platform-specific categories
    /// EXAMPLES: Core data structures, fundamental algorithms, basic infrastructure tests
    /// </summary>
    Core = 1L << 1,

    /// <summary>
    /// Tests for client API functionality and document store operations.
    /// USAGE: Tests involving DocumentStore, Session operations, basic CRUD operations.
    /// COMBINE WITH: Most other categories as this is foundational
    /// EXAMPLES: Document loading, storing, querying, session management, conventions
    /// </summary>
    [Description("Client API")]
    ClientApi = 1L << 2,

    /// <summary>
    /// Tests for server and database configuration.
    /// USAGE: Tests involving configuration settings, server options, database settings.
    /// COMBINE WITH: Setup, Security, Memory
    /// EXAMPLES: Configuration validation, setting changes, configuration inheritance
    /// </summary>
    Configuration = 1L << 3,

    /// <summary>
    /// Tests for server and database setup procedures.
    /// USAGE: Tests involving server initialization, database creation, initial configuration.
    /// COMBINE WITH: Configuration, Security, Certificates
    /// EXAMPLES: Server startup, database setup, initial data loading, setup wizards
    /// </summary>
    Setup = 1L << 4,

    /// <summary>
    /// Tests that check codebase sanity checks.
    /// USAGE: Tests involving checking inheritance rules applied by the project.
    /// EXAMPLES: Inheritance, testing attributes to the tests, non-disposable tests, etc.
    /// </summary>
    Codebase = 1L << 5,

    // ===== PLATFORM & ARCHITECTURE =====

    /// <summary>
    /// Tests specific to Linux platform.
    /// USAGE: Tests that should only run on Linux or test Linux-specific functionality.
    /// COMBINE WITH: Core, Memory, Voron for platform-specific behavior
    /// EXAMPLES: Linux-specific file operations, Linux performance characteristics
    /// REQUIREMENTS: Use RavenMultiplatformFact with RavenPlatform.Linux
    /// </summary>
    Linux = 1L << 6,

    /// <summary>
    /// Tests specific to Windows platform.
    /// USAGE: Tests that should only run on Windows or test Windows-specific functionality.
    /// COMBINE WITH: Core, Memory, Voron for platform-specific behavior
    /// EXAMPLES: Windows-specific file operations, Windows performance characteristics, Windows services
    /// REQUIREMENTS: Use RavenMultiplatformFact with RavenPlatform.Windows
    /// </summary>
    Windows = 1L << 7,

    /// <summary>
    /// Tests for Platform Abstraction Layer (PAL).
    /// USAGE: Tests involving platform-specific abstractions, cross-platform compatibility.
    /// COMBINE WITH: Linux, Windows, Core
    /// EXAMPLES: Platform-specific implementations, PAL interface tests
    /// </summary>
    [Description("PAL")]
    Pal = 1L << 8,

    /// <summary>
    /// Tests for CPU intrinsics and SIMD functionality.
    /// USAGE: Tests involving CPU-specific optimizations, SIMD operations, intrinsic functions.
    /// COMBINE WITH: Memory, Core, platform/architecture-specific categories
    /// EXAMPLES: SIMD operations, CPU intrinsic usage, performance optimizations
    /// REQUIREMENTS: Often requires specific CPU architecture restrictions
    /// </summary>
    Intrinsics = 1L << 9,

    // ===== STORAGE & MEMORY =====

    /// <summary>
    /// Tests for Voron storage engine functionality.
    /// USAGE: Tests involving low-level storage operations, Voron-specific features, storage performance.
    /// COMBINE WITH: Memory, Core, Performance-related categories
    /// EXAMPLES: Voron transactions, Voron storage operations, Voron performance, storage corruption handling
    /// REQUIREMENTS: Often requires specific platform/architecture restrictions
    /// </summary>
    Voron = 1L << 10,

    /// <summary>
    /// Tests for memory management and memory-related functionality.
    /// USAGE: Tests involving memory allocation, memory pressure, memory optimization.
    /// COMBINE WITH: Voron, Core, Performance-related categories
    /// EXAMPLES: Memory usage tests, memory leak detection, memory pressure scenarios
    /// REQUIREMENTS: Often requires specific platform/architecture restrictions
    /// </summary>
    Memory = 1L << 11,

    /// <summary>
    /// Tests for data compression features.
    /// USAGE: Tests involving document compression, backup compression, storage optimization.
    /// COMBINE WITH: BackupExportImport, Memory, Configuration
    /// EXAMPLES: Document compression settings, compressed backups, compression performance
    /// REQUIREMENTS: May require LicenseRequired = true for advanced compression features
    /// </summary>
    Compression = 1L << 12,

    // ===== SEARCH & QUERYING =====

    /// <summary>
    /// Tests for indexing functionality, server-side indexes, auto indexes, index errors, index performance, map-reduce mechanism, etc.
    /// USAGE: Tests involving index creation, index errors, index management.
    /// MAY COMBINE WITH: Querying, JavaScript, Corax, Lucene, Spatial, Counters, TimeSeries
    /// EXAMPLES: Static indexes, auto indexes, map-reduce indexes, multi-maps, index errors, index performance
    /// </summary>
    Indexes = 1L << 13,

    /// <summary>
    /// Tests for ensuring correctness of the query results, like ordering, filtering, projections, etc.
    /// USAGE: Tests involving RQL queries, LINQ queries, query optimization.
    /// MAY COMBINE WITH: Indexes, Rql, Spatial, Facets, ClientApi
    /// EXAMPLES: Query syntax, query performance, query results, dynamic queries, custom queries, includes, ordering, etc.
    /// </summary>
    Querying = 1L << 14,

    /// <summary>
    /// Tests for RavenDB Query Language (RQL) specific functionality.
    /// USAGE: Tests involving raw RQL queries, RQL syntax, RQL features.
    /// COMBINE WITH: Querying, Indexes
    /// EXAMPLES: RQL syntax validation, RQL query execution, RQL-specific features
    /// </summary>
    [Description("RQL")]
    Rql = 1L << 15,

    /// <summary>
    /// Tests for faceted search and aggregation.
    /// USAGE: Tests involving facet queries, aggregations, facet-based filtering.
    /// COMBINE WITH: Querying, Indexes
    /// EXAMPLES: Facet definitions, range facets, facet aggregations, dynamic facets
    /// </summary>
    Facets = 1L << 16,

    /// <summary>
    /// Tests for spatial/geographic functionality.
    /// USAGE: Tests involving spatial queries, geographic data, spatial indexing.
    /// COMBINE WITH: Indexes, Querying
    /// EXAMPLES: Spatial queries, geographic shapes, spatial indexing, distance calculations
    /// </summary>
    Spatial = 1L << 17,

    /// <summary>
    /// Tests for search result highlighting functionality.
    /// USAGE: Tests involving search term highlighting, highlighted snippets, highlighting configuration.
    /// COMBINE WITH: Querying, Indexes, Lucene, Corax
    /// EXAMPLES: Search highlighting, highlighted fragments, highlighting customization
    /// </summary>
    Highlighting = 1L << 18,

    /// <summary>
    /// Tests for Lucene search engine functionality.
    /// USAGE: Tests involving Lucene search engine, Lucene-specific features, Lucene performance.
    /// COMBINE WITH: Indexes, Querying, Spatial, Facets, Highlighting
    /// EXAMPLES: Lucene indexing, Lucene queries, Lucene analyzers, Lucene vs Corax comparisons
    /// </summary>
    Lucene = 1L << 19,

    /// <summary>
    /// Tests for Corax search engine functionality.
    /// USAGE: Tests involving Corax search engine, Corax-specific features, Corax performance.
    /// COMBINE WITH: Indexes, Querying, Spatial, Facets
    /// EXAMPLES: Corax indexing, Corax queries, Corax vs Lucene comparisons, Corax-specific features
    /// </summary>
    Corax = 1L << 20,

    /// <summary>
    /// Tests for vector search and AI functionality.
    /// USAGE: Tests involving vector operations, vector indexing, vector queries.
    /// COMBINE WITH: Indexes, Querying, Ai
    /// EXAMPLES: Vector similarity search, vector indexing, vector operations
    /// </summary>
    Vector = 1L << 21,

    /// <summary>
    /// Tests for AI and machine learning functionality.
    /// USAGE: Tests involving AI features, machine learning operations, AI integrations.
    /// COMBINE WITH: Vector, Indexes, Querying
    /// EXAMPLES: AI-powered features, ML model integration, AI query processing
    /// </summary>
    Ai = 1L << 22,

    // ===== DOCUMENT FEATURES =====

    /// <summary>
    /// Tests for document attachments functionality.
    /// USAGE: Tests involving attachment storage, retrieval, operations (Store, Get, Delete, Copy, Move).
    /// COMBINE WITH: ClientApi, BackupExportImport, Sharding, Replication
    /// EXAMPLES: Attachment CRUD operations, attachment metadata, attachment streaming
    /// </summary>
    Attachments = 1L << 23,

    /// <summary>
    /// Tests for bulk insert operations. Automatically includes ClientApi.
    /// USAGE: Tests involving BulkInsert API, high-volume data insertion, bulk operations performance.
    /// COMBINE WITH: Attachments, TimeSeries, Counters
    /// EXAMPLES: Bulk document insertion, bulk attachment operations, bulk insert with time series
    /// </summary>
    BulkInsert = 1L << 24 | ClientApi,

    /// <summary>
    /// Tests for counter functionality.
    /// USAGE: Tests involving document counters, counter operations, counter replication.
    /// COMBINE WITH: ClientApi, Replication, Cluster
    /// EXAMPLES: Counter increment/decrement, counter queries, distributed counters
    /// </summary>
    Counters = 1L << 25,

    /// <summary>
    /// Tests for time series functionality.
    /// USAGE: Tests involving time series data, time series queries, time series operations.
    /// COMBINE WITH: ClientApi, Indexes, Querying, BulkInsert
    /// EXAMPLES: Time series append, time series queries, time series aggregation, time series indexing
    /// </summary>
    TimeSeries = 1L << 26,

    /// <summary>
    /// Tests for document revisions functionality.
    /// USAGE: Tests involving document history, revision tracking, revision queries.
    /// COMBINE WITH: ClientApi, Configuration, BackupExportImport
    /// EXAMPLES: Revision configuration, revision retrieval, revision cleanup, revision metadata
    /// </summary>
    Revisions = 1L << 27,

    /// <summary>
    /// Tests for document expiration and refresh functionality.
    /// USAGE: Tests involving document TTL, expiration policies, document refresh.
    /// COMBINE WITH: Configuration, Cluster
    /// EXAMPLES: Document expiration settings, expired document cleanup, expiration in clusters
    /// </summary>
    ExpirationRefresh = 1L << 28,

    /// <summary>
    /// Tests for document patching operations.
    /// USAGE: Tests involving document patches, patch scripts, patch operations.
    /// COMBINE WITH: JavaScript, ClientApi
    /// EXAMPLES: JavaScript patches, patch by query, patch operations, patch testing
    /// </summary>
    Patching = 1L << 29,

    /// <summary>
    /// Tests for JavaScript functionality in RavenDB.
    /// USAGE: Tests involving JavaScript patches, JavaScript indexes, admin console scripts.
    /// COMBINE WITH: Patching, Indexes, Etl
    /// EXAMPLES: JavaScript patches, JavaScript index definitions, custom JavaScript functions
    /// </summary>
    JavaScript = 1L << 30,

    // ===== CLUSTER & DISTRIBUTION =====

    /// <summary>
    /// Tests for cluster functionality and multi-node operations.
    /// USAGE: Tests involving cluster setup, node management, cluster-wide operations.
    /// COMBINE WITH: Replication, ClusterTransactions, Sharding, Certificates
    /// EXAMPLES: Cluster formation, leader election, cluster database operations, node failover
    /// REQUIREMENTS: Often requires multi-node test setup
    /// </summary>
    Cluster = 1L << 31,

    /// <summary>
    /// Tests for sharding functionality.
    /// USAGE: Tests involving sharded databases, shard management, cross-shard operations.
    /// COMBINE WITH: Cluster, Replication, BackupExportImport, most other categories
    /// EXAMPLES: Shard creation, shard key configuration, cross-shard queries, shard rebalancing
    /// REQUIREMENTS: Automatically skipped on 32-bit platforms, often requires cluster setup
    /// </summary>
    Sharding = 1L << 32,

    /// <summary>
    /// Tests for replication functionality.
    /// USAGE: Tests involving database replication, external replication, replication conflicts.
    /// COMBINE WITH: Cluster, Etl, Certificates, Licensing
    /// EXAMPLES: External replication setup, replication conflicts, pull replication, hub replication
    /// REQUIREMENTS: May require LicenseRequired = true for advanced replication features
    /// </summary>
    Replication = 1L << 33,

    /// <summary>
    /// Tests for compare exchange functionality.
    /// USAGE: Tests involving compare exchange operations, distributed coordination, atomic operations.
    /// COMBINE WITH: Cluster, ClientApi
    /// EXAMPLES: Compare exchange operations, distributed locks, atomic counters, cluster coordination
    /// </summary>
    CompareExchange = 1L << 34,

    /// <summary>
    /// Tests for cluster-wide transactions functionality.
    /// USAGE: Tests involving cluster-wide ACID transactions, distributed transaction coordination.
    /// COMBINE WITH: Cluster, CompareExchange, BackupExportImport
    /// EXAMPLES: Cluster-wide transactions, distributed transaction rollback, cluster transaction performance
    /// REQUIREMENTS: Requires cluster setup
    /// </summary>
    ClusterTransactions = 1L << 35,

    /// <summary>
    /// Tests for subscription functionality.
    /// USAGE: Tests involving data subscriptions, subscription processing, subscription management.
    /// COMBINE WITH: ClientApi, Cluster, Sharding, Certificates
    /// EXAMPLES: Subscription creation, subscription processing, subscription failover, subscription filtering
    /// PURPOSE: Guaranteed ongoing data processing and batch operations
    /// </summary>
    Subscriptions = 1L << 36,

    /// <summary>
    /// Tests for Changes API functionality.
    /// USAGE: Tests involving real-time change notifications, change streams, change filtering.
    /// COMBINE WITH: ClientApi, Subscriptions
    /// EXAMPLES: Document changes, index changes, operation changes, change filtering
    /// PURPOSE: Immediate notifications about database events
    /// </summary>
    [Description("Changes API")]
    ChangesApi = 1L << 37,

    // ===== DATA INTEGRATION =====

    /// <summary>
    /// Tests for Extract, Transform, Load (ETL) operations.
    /// USAGE: Tests involving ETL processes, data transformation, external system integration.
    /// COMBINE WITH: Replication, JavaScript, PostgreSql
    /// EXAMPLES: RavenDB ETL, SQL ETL, OLAP ETL, ETL transformations
    /// REQUIREMENTS: May require external system setup for integration tests
    /// </summary>
    [Description("ETL")]
    Etl = 1L << 38,

    /// <summary>
    /// Tests for sink functionality (ETL destinations, replication targets).
    /// USAGE: Tests involving ETL sinks, replication sinks, data destination configuration.
    /// COMBINE WITH: Etl, Replication
    /// EXAMPLES: ETL sink configuration, sink data processing, sink error handling
    /// REQUIREMENTS: May require LicenseRequired = true for advanced sink features
    /// </summary>
    Sinks = 1L << 39,

    /// <summary>
    /// Tests for PostgreSQL integration and ETL.
    /// USAGE: Tests involving PostgreSQL ETL, PostgreSQL connections, SQL integration.
    /// COMBINE WITH: Etl
    /// EXAMPLES: PostgreSQL ETL setup, PostgreSQL data transformation, SQL ETL operations
    /// REQUIREMENTS: May require PostgreSQL server setup for integration tests
    /// </summary>
    [Description("PostgreSQL")]
    PostgreSql = 1L << 40,

    /// <summary>
    /// Tests for Power BI integration.
    /// USAGE: Tests involving Power BI connectivity, Power BI data sources.
    /// COMBINE WITH: Etl, Querying
    /// EXAMPLES: Power BI connector tests, Power BI data export
    /// REQUIREMENTS: May require Power BI setup for integration tests
    /// </summary>
    [Description("Power BI")]
    PowerBi = 1L << 41,

    /// <summary>
    /// Tests for backup, export, and import operations.
    /// USAGE: Tests involving database backup/restore, smuggler operations, data migration.
    /// COMBINE WITH: Attachments, Sharding, Cluster, Encryption, Compression
    /// EXAMPLES: Periodic backups, incremental backups, cross-database imports, backup encryption
    /// </summary>
    BackupExportImport = 1L << 42,

    /// <summary>
    /// Tests for smuggler (import/export) functionality.
    /// USAGE: Tests involving data import/export, smuggler operations, data migration tools.
    /// COMBINE WITH: BackupExportImport, Attachments, Indexes
    /// EXAMPLES: Data export, data import, smuggler filtering, cross-version migration
    /// </summary>
    Smuggler = 1L << 43,

    // ===== SECURITY & LICENSING =====

    /// <summary>
    /// Tests for security functionality.
    /// USAGE: Tests involving authentication, authorization, security policies, access control.
    /// COMBINE WITH: Certificates, Configuration, Cluster
    /// EXAMPLES: User authentication, database access control, security configuration, API security
    /// </summary>
    Security = 1L << 44,

    /// <summary>
    /// Tests for SSL/TLS certificates and certificate management.
    /// USAGE: Tests involving client certificates, server certificates, certificate authentication.
    /// COMBINE WITH: Security, Cluster, Encryption
    /// EXAMPLES: Certificate-based authentication, certificate rotation, cluster certificate setup
    /// REQUIREMENTS: Often requires certificate setup in test infrastructure
    /// </summary>
    Certificates = 1L << 45,

    /// <summary>
    /// Tests for database encryption features.
    /// USAGE: Tests involving encrypted databases, encryption at rest, encrypted backups.
    /// COMBINE WITH: BackupExportImport, Certificates, Sharding, Security
    /// EXAMPLES: Encrypted database creation, encrypted backup/restore, key management
    /// REQUIREMENTS: LicenseRequired = true, special encryption setup
    /// </summary>
    Encryption = 1L << 46,

    /// <summary>
    /// Tests for licensing functionality and license validation.
    /// USAGE: Tests involving license checks, license limits, license enforcement.
    /// COMBINE WITH: Most categories when testing license restrictions
    /// EXAMPLES: License validation, feature restrictions, license upgrades/downgrades
    /// REQUIREMENTS: Often requires RavenMultiLicenseRequiredFact with multiple license types
    /// </summary>
    Licensing = 1L << 47,

    // ===== OPERATIONS & MONITORING =====

    /// <summary>
    /// Tests for RavenDB Studio functionality.
    /// USAGE: Tests involving Studio UI, Studio API endpoints, Studio features.
    /// COMBINE WITH: ClientApi, Configuration
    /// EXAMPLES: Studio endpoints, Studio data visualization, Studio configuration
    /// </summary>
    Studio = 1L << 48,

    /// <summary>
    /// Tests for logging functionality.
    /// USAGE: Tests involving log configuration, log output, log analysis.
    /// COMBINE WITH: Configuration, Monitoring
    /// EXAMPLES: Log level settings, log format validation, log rotation
    /// </summary>
    Logging = 1L << 49,

    /// <summary>
    /// Tests for monitoring and metrics functionality.
    /// USAGE: Tests involving performance monitoring, metrics collection, monitoring endpoints.
    /// COMBINE WITH: Configuration, Logging
    /// EXAMPLES: Performance metrics, monitoring endpoints, metrics collection, alerting
    /// </summary>
    Monitoring = 1L << 50,

    /// <summary>
    /// Tests for embedded RavenDB functionality.
    /// USAGE: Tests involving embedded server, embedded database operations, embedded configuration.
    /// COMBINE WITH: Licensing, Configuration, ClientApi
    /// EXAMPLES: Embedded server startup, embedded database operations, embedded licensing
    /// REQUIREMENTS: Often requires special embedded test setup
    /// </summary>
    Embedded = 1L << 51,

    /// <summary>
    /// Tests for interversion compatibility and upgrades.
    /// USAGE: Tests involving version compatibility, upgrade scenarios, mixed version clusters.
    /// COMBINE WITH: Cluster, BackupExportImport
    /// EXAMPLES: Version upgrade tests, mixed version clusters, compatibility validation
    /// REQUIREMENTS: Often requires multiple RavenDB versions, special test infrastructure
    /// </summary>
    Interversion = 1L << 52,
}
