import { createOpenApiHttp } from "openapi-msw";
import type { paths } from "@/api/generated/server-api";

// MSW `http` bound to the generated OpenAPI types: endpoint paths autocomplete and
// params/request/response bodies are type-checked against the server contract.
export const apiHttp = createOpenApiHttp<paths>();
