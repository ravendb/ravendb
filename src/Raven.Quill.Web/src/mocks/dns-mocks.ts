import { apiHttp } from "./api-http";

export const dnsMocks = {
    ipBinding: (addresses: string[] = ["51.210.14.7"]) =>
        apiHttp.get("/api/dns/ip-binding", ({ response }) =>
            response(200).json({ hostname: "dashboard.acme.ravendb.run", addresses }),
        ),
    ipBindingError: () =>
        apiHttp.get("/api/dns/ip-binding", ({ response }) =>
            response(502).json({ error: "DNS lookup for 'dashboard.acme.ravendb.run' failed" }),
        ),
};
