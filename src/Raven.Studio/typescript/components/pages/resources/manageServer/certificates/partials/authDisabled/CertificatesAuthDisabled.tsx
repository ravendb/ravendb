import { Icon } from "components/common/Icon";
import RichAlert from "components/common/RichAlert";
import { useRavenLink } from "components/hooks/useRavenLink";
import { Card } from "reactstrap";

export default function CertificatesAuthDisabled() {
    const certificatesDocsLink = useRavenLink({ hash: "RSFSL5" });

    return (
        <Card className="p-4 rounded w-75 m-auto">
            <h2 className="text-warning">
                <Icon icon="unsecure" /> Authentication is disabled
            </h2>
            RavenDB uses certificates to authenticate clients, but the server certificate information has not been set
            up.
            <hr />
            <strong>In order to set up authentication for your server, proceed with the following:</strong>
            <ol>
                <li>
                    Locate the <code>settings.json</code> file in your server directory.
                </li>
                <li>
                    Enter your .pfx certificate path under <code>Security.Certificate.Path</code>, or if you are using
                    an executable or command that returns a .pfx file, place it under{" "}
                    <code>Security.Certificate.Load.Exec</code>.
                </li>
                <li>Save and restart the server to apply the changes.</li>
            </ol>
            <RichAlert variant="info">
                If either option is specified, RavenDB will use <i>HTTPS/SSL</i> for all network activities.
                <br />
                The certificate path setting takes precedence over executable configuration options.
            </RichAlert>
            <hr />
            <span>
                For more information please see{" "}
                <a href={certificatesDocsLink} target="_blank">
                    RavenDB Documentation <Icon icon="newtab" />
                </a>
            </span>
        </Card>
    );
}
