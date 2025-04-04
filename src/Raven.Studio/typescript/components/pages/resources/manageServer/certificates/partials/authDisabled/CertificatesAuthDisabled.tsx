import { Icon } from "components/common/Icon";
import RichAlert from "components/common/RichAlert";
import { useRavenLink } from "components/hooks/useRavenLink";
import { NumberedList, NumberedListItem } from "components/common/NumberedList";
import Card from "react-bootstrap/Card";
import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";

export default function CertificatesAuthDisabled() {
    const certificatesDocsLink = useRavenLink({ hash: "RSFSL5" });

    return (
        <Row>
            <Col sm={12} lg={7} className="mx-auto">
                <Card className="p-5 rounded">
                    <h3 className="text-warning mb-1">
                        <Icon icon="unsecure" /> Authentication is disabled
                    </h3>
                    <p className="lead mb-0">
                        RavenDB uses certificates to authenticate clients, but the server certificate information has
                        not been set up.
                    </p>
                    <hr />
                    <strong>In order to set up authentication for your server, proceed with the following:</strong>
                    <NumberedList className="mt-3 mb-4">
                        <NumberedListItem stepKey={1}>
                            Locate the <code>settings.json</code> file in your server directory.
                        </NumberedListItem>
                        <NumberedListItem stepKey={2}>
                            Enter your .pfx certificate path under <code>Security.Certificate.Path</code>, or if you are
                            using an executable or command that returns a .pfx file, place it under{" "}
                            <code>Security.Certificate.Load.Exec</code>.
                        </NumberedListItem>
                        <NumberedListItem stepKey={3}>
                            Please ensure that all <code>ServerUrl</code> configurations use HTTPS protocol instead of
                            HTTP.
                        </NumberedListItem>
                        <NumberedListItem stepKey={4}>
                            Save and restart the server to apply the changes.
                        </NumberedListItem>
                    </NumberedList>
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
            </Col>
        </Row>
    );
}
