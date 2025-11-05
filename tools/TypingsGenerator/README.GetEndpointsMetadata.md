# GET Endpoints Metadata Generator

## Overview

The `GetEndpointsMetadataExporter` is a tool that scans all RavenDB GET endpoints and generates comprehensive JSON documentation for each endpoint.

## Output Format

The tool generates a JSON file (`get-endpoints-metadata.json`) containing an array of endpoint metadata objects with the following structure:

```json
{
  "Path": "/databases/{database}/indexes/terms",
  "Method": "GET",
  "Description": "Returns all terms in a specified index field for introspection or auto-complete.",
  "Tags": ["Indexing", "Diagnostics"],
  "QueryParams": [
    {
      "Name": "name",
      "Required": true,
      "Description": "Name of index."
    },
    {
      "Name": "field",
      "Required": true,
      "Description": "Index field to extract terms from."
    }
  ]
}
```

## Fields

- **Path**: The endpoint URL path (PascalCase key)
- **Method**: HTTP method (always "GET" for this tool) (PascalCase key)
- **Description**: Human-readable description of the endpoint's purpose (PascalCase key)
  - Can be set explicitly via the `Description` property on `RavenActionAttribute`
  - Falls back to auto-generated description based on method names and path patterns if not set
- **Tags**: Array of category tags for the endpoint (PascalCase key)
- **QueryParams**: Array of query parameter definitions (PascalCase key)
  - **Name**: Parameter name (PascalCase key)
  - **Required**: Whether the parameter is required (PascalCase key)
  - **Description**: Parameter description (PascalCase key)

## Adding Descriptions to Endpoints

The best way to provide accurate descriptions is to add them directly to the `RavenActionAttribute`:

```csharp
[RavenAction("/databases/*/indexes/terms", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, 
    DisableOnCpuCreditsExhaustion = true,
    Description = "Returns all terms in a specified index field for introspection or auto-complete.")]
public async Task Terms()
{
    // ...
}
```

If no description is provided via the attribute, the tool will auto-generate one based on the method name and path patterns.

## Tags

Endpoints are automatically tagged based on their path and functionality:

- **Indexing**: Index-related operations
- **Query**: Query execution endpoints
- **Documents**: Document operations
- **Replication**: Replication endpoints
- **Cluster**: Cluster management
- **Diagnostics**: Stats, metrics, and monitoring
- **Admin**: Administrative operations
- **Debug**: Debug and troubleshooting
- **Security**: Certificates and authentication
- **ETL**: Extract, Transform, Load operations
- **Backup**: Backup and restore
- **Subscriptions**: Subscription management
- **Configuration**: Configuration endpoints
- **Memory**: Memory management
- **Network**: Network and TCP operations
- **Studio**: Studio-specific endpoints
- And more...

## Usage

The tool is integrated into the TypingsGenerator and runs automatically when the TypingsGenerator is executed:

```bash
cd tools/TypingsGenerator
dotnet run -c Release
```

This will generate the file at:
```
src/Raven.Studio/typings/server/get-endpoints-metadata.json
```

## Implementation Details

The tool:

1. Uses reflection to scan all types inheriting from `RequestHandler`
2. Finds methods decorated with `RavenActionAttribute` where the HTTP method is "GET"
3. Extracts endpoint metadata including:
   - Path from the attribute
   - Description based on method name and path patterns
   - Tags based on path analysis
   - Query parameters from common patterns and known endpoints
4. Outputs JSON with PascalCase property names

## Query Parameter Extraction

Query parameters are determined through:

1. **Pattern matching**: Common endpoints like `/indexes/terms`, `/queries`, etc. have well-defined parameter sets
2. **Known mappings**: Frequently used parameters are mapped based on endpoint patterns
3. **Method name analysis**: Method names provide hints about expected parameters

### Common Parameters

- `start`, `pageSize`: Pagination parameters for list endpoints
- `name`: Resource identifier for many operations
- `debugInfo`, `details`: Debug and detailed information flags
- `id`: Document or resource identifier
- `field`, `collection`: Index and collection filters

## Adding New Parameter Mappings

To add query parameter mappings for new endpoints, edit the `GetKnownParametersForPath` method in `GetEndpointsMetadataExporter.cs`. Add path pattern matching and parameter definitions for the new endpoints.

Example:
```csharp
if (path.Contains("/new-endpoint"))
{
    parameters.Add(new QueryParameter 
    { 
        Name = "paramName", 
        Required = true, 
        Description = "Parameter description." 
    });
}
```

## Statistics

As of the latest run, the tool documents **312 GET endpoints** across the RavenDB API.

## Output Location

The generated file is placed in:
```
src/Raven.Studio/typings/server/get-endpoints-metadata.json
```

This file can be used by:
- API documentation generators
- Client SDK generators
- Testing tools
- API exploration interfaces
- Developer documentation

## Notes

- The tool only documents GET endpoints; other HTTP methods are excluded
- All JSON keys use PascalCase as specified in requirements
- Query parameters are based on known patterns; some less common parameters may not be included
- The generated file should be committed to the repository as documentation
