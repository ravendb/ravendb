import { http, HttpResponse } from "msw";

// Mirrors the DNS-over-HTTPS lookup the IP configuration page performs.
export const dnsMocks = {
    resolve: (ips: string[] = ["51.210.14.7"]) =>
        http.get("https://dns.google/resolve", () =>
            HttpResponse.json({
                Status: 0,
                Answer: ips.map((ip) => ({ type: 1, data: ip })),
            }),
        ),
    resolveError: () => http.get("https://dns.google/resolve", () => HttpResponse.json({ Status: 2 })),
};
