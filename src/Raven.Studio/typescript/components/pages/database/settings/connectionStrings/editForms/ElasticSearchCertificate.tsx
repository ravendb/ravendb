import React from "react";

interface ElasticSearchCertificateProps {
    base64: string;
}

export default function ElasticSearchCertificate({ base64 }: ElasticSearchCertificateProps) {
    return <div>{base64}</div>;
}
