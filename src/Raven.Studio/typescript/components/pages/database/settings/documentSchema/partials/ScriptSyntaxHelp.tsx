import Code from "components/common/Code";
import React from "react";

export function ScriptSyntaxHelp() {
    const employeeSchema = `
{
    "title": "Employee",
    "type": "object",
    "properties": {
        "FirstName": { "type": "string" },
        "LastName": { "type": "string" },
        "Title": { "type": "string" },
        "Address": {
            "type": "object",
            "properties": {
                "City": { "type": "string" },
                "Country": { "type": "string" },
                "Line1": { "type": "string" },
                "Line2": { "type": ["string", "null"] },
                "Location": {
                    "type": ["object", "null"],
                    "properties": {
                        "Latitude": { "type": "number" },
                        "Longitude": { "type": "number" }
                    }
                },
                "PostalCode": { "type": "string" },
                "Region": { "type": ["string", "null"] }
            },
            "required": ["City", "Country", "Line1", "PostalCode", "Region"]
        },
        "Birthday": { "type": "string", "format": "date-time" },
        "HiredAt": { "type": "string", "format": "date-time" },
        "ReportsTo": {
            "type": ["string", "null"],
            "pattern": "^employees/\\\\d+-[A-Z]$"
        },
        "HomePhone": {
            "type": ["string", "null"],
            "pattern": "^\\\\(\\\\d{1,3}\\\\)\\\\s?\\\\d{3}-\\\\d{4}$",
            "description": "Phone number in the format (206) 555-1189 or (71) 555-4848."
        },
        "Notes": {
            "type": "array",
            "items": {
                "type": "string",
                "minLength": 10,
                "maxLength": 1000
            },
            "maxItems": 10
        }
    },
    "required": ["FirstName", "LastName", "Title", "Address", "HomePhone"],
    "additionalProperties": true
}`;

    return (
        <div>
            <div>
                Sample schema for a document in the <code>Employees</code> collection:
            </div>
            <Code code={employeeSchema} language="json" />
        </div>
    );
}
